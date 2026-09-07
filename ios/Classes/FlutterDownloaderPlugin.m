#import "FlutterDownloaderPlugin.h"
#import "DBManager.h"
#import <AVFoundation/AVFoundation.h>
#import <math.h>

#define STATUS_UNDEFINED 0
#define STATUS_ENQUEUED 1
#define STATUS_RUNNING 2
#define STATUS_COMPLETE 3
#define STATUS_FAILED 4
#define STATUS_CANCELED 5
#define STATUS_PAUSED 6

#define KEY_URL @"url"
#define KEY_SAVED_DIR @"saved_dir"
#define KEY_SEARCH_DIR @"search_dir"
#define KEY_FILE_NAME @"file_name"
#define KEY_PROGRESS @"progress"
#define KEY_ID @"id"
#define KEY_IDS @"ids"
#define KEY_TASK_ID @"task_id"
#define KEY_STATUS @"status"
#define KEY_HEADERS @"headers"
#define KEY_RESUMABLE @"resumable"
#define KEY_SHOW_NOTIFICATION @"show_notification"
#define KEY_OPEN_FILE_FROM_NOTIFICATION @"open_file_from_notification"
#define KEY_QUERY @"query"
#define KEY_TIME_CREATED @"time_created"
#define KEY_CONTENT_ID @"content_id"
#define KEY_LOCAL_FILE_PATH @"local_file_path"
#define KEY_QUALITY_HEIGHT @"quality_height"
#define KEY_TITLE @"title"
#define KEY_ALLOW_CELLULAR @"allow_cellular"

#define NULL_VALUE @"<null>"

#define ERROR_NOT_INITIALIZED [FlutterError errorWithCode:@"not_initialized" message:@"initialize() must called first" details:nil]
#define ERROR_INVALID_TASK_ID [FlutterError errorWithCode:@"invalid_task_id" message:@"not found task corresponding to given task id" details:nil]

@interface FlutterDownloaderPlugin()<NSURLSessionTaskDelegate, NSURLSessionDownloadDelegate, AVAssetDownloadDelegate, UIDocumentInteractionControllerDelegate>
{
    FlutterMethodChannel *_mainChannel;
    FlutterMethodChannel *_callbackChannel;
    NSObject<FlutterPluginRegistrar> *_registrar;
    DBManager *_dbManager;
    NSString *_allFilesDownloadedMsg;
    NSMutableArray *_eventQueue;
    NSMutableDictionary<NSString *, void (^)(void)> *_backgroundCompletionHandlers;
}

@property(nonatomic, strong) dispatch_queue_t databaseQueue;

/// The flag ensures that the database task avoids be marked as other status after be marked as canceled in the termination.
@property(nonatomic, assign, getter=isDatabaseQueueTerminated) BOOL databaseQueueTerminated;

@end

@implementation FlutterDownloaderPlugin

static FlutterPluginRegistrantCallback registerPlugins = nil;
static BOOL initialized = NO;
static BOOL debug = YES;
static NSURLSession *_session = nil;
static AVAssetDownloadURLSession *_hlsSession = nil;
static FlutterEngine *_headlessRunner = nil;
static int64_t _callbackHandle = 0;
static int _step = 10;
static NSMutableDictionary<NSString*, NSMutableDictionary*> *_runningTaskById = nil;

static NSSearchPathDirectory const kDefaultSearchPathDirectory = NSDocumentDirectory;

@synthesize databaseQueue;

- (instancetype)init:(NSObject<FlutterPluginRegistrar> *)registrar;
{
    if (self = [super init]) {
        BOOL _isolate = NO;
        if (_headlessRunner == nil) {
            _headlessRunner = [[FlutterEngine alloc] initWithName:@"FlutterDownloaderIsolate" project:nil allowHeadlessExecution:YES];
        } else {
            _isolate = YES;
        }

        _registrar = registrar;

        _mainChannel = [FlutterMethodChannel
                           methodChannelWithName:@"vn.hunghd/downloader"
                           binaryMessenger:[registrar messenger]];
        [registrar addMethodCallDelegate:self channel:_mainChannel];

        _callbackChannel =
        [FlutterMethodChannel methodChannelWithName:@"vn.hunghd/downloader_background"
                                    binaryMessenger:[_headlessRunner binaryMessenger]];

        _eventQueue = [[NSMutableArray alloc] init];
        _backgroundCompletionHandlers = [[NSMutableDictionary alloc] init];

        NSBundle *frameworkBundle = [NSBundle bundleForClass:FlutterDownloaderPlugin.class];

        // initialize Database
        NSURL *bundleUrl = [[frameworkBundle resourceURL] URLByAppendingPathComponent:@"FlutterDownloaderDatabase.bundle"];
        NSBundle *resourceBundle = [NSBundle bundleWithURL:bundleUrl];
        NSString *dbPath = [resourceBundle pathForResource:@"download_tasks" ofType:@"sql"];
        if (debug) {
            NSLog(@"database path: %@", dbPath);
        }
        databaseQueue = dispatch_queue_create("vn.hunghd.flutter_downloader", 0);
        
        _dbManager = [[DBManager alloc] initWithDatabaseFilePath:dbPath];
        
        __typeof__(self) __weak weakSelf = self;
        [self executeInDatabaseQueueForTask:^{
            [weakSelf addDatabaseColumnForMakingFileCouldSaveInAnyDirectory];
            [weakSelf addDatabaseColumnsForHlsDownloads];
        }];
        
        if (_runningTaskById == nil) {
            _runningTaskById = [[NSMutableDictionary alloc] init];
        }

        // The HLS session is owned by the main plugin instance so iOS can
        // reconnect it immediately when relaunching the app for background
        // asset-download events.
        [self initializeHlsSessionIfNeeded];

        NSBundle *mainBundle = [NSBundle mainBundle];

        // init NSURLSession in background isolate
        if (_isolate) {
            NSNumber *maxConcurrentTasks = [mainBundle objectForInfoDictionaryKey:@"FDMaximumConcurrentTasks"];
            if (maxConcurrentTasks == nil) {
                maxConcurrentTasks = @3;
            }
            if (debug) {
                NSLog(@"MAXIMUM_CONCURRENT_TASKS = %@", maxConcurrentTasks);
            }
            // session identifier needs to be the same for background download and resume to work
            NSString *identifier = [NSString stringWithFormat:@"%@.download.background.session", NSBundle.mainBundle.bundleIdentifier];
            NSURLSessionConfiguration *sessionConfiguration = [NSURLSessionConfiguration backgroundSessionConfigurationWithIdentifier:identifier];
            sessionConfiguration.HTTPMaximumConnectionsPerHost = [maxConcurrentTasks intValue];
            _session = [NSURLSession sessionWithConfiguration:sessionConfiguration delegate:self delegateQueue:nil];
            if (debug) {
                NSLog(@"init NSURLSession with id: %@", [[_session configuration] identifier]);
            }
        }

        _allFilesDownloadedMsg = [mainBundle objectForInfoDictionaryKey:@"FDAllFilesDownloadedMessage"];
        if (_allFilesDownloadedMsg == nil) {
            _allFilesDownloadedMsg = @"All files have been downloaded";
        }
        if (debug) {
            NSLog(@"AllFilesDownloadedMessage: %@", _allFilesDownloadedMsg);
        }
    }

    return self;
}

- (void)startBackgroundIsolate:(int64_t)handle {
    if (debug) {
        NSLog(@"startBackgroundIsolate");
    }
    FlutterCallbackInformation *info = [FlutterCallbackCache lookupCallbackInformation:handle];
    NSAssert(info != nil, @"failed to find callback");
    NSString *entrypoint = info.callbackName;
    NSString *uri = info.callbackLibraryPath;
    [_headlessRunner runWithEntrypoint:entrypoint libraryURI:uri];
    NSAssert(registerPlugins != nil, @"failed to set registerPlugins");

    // Once our headless runner has been started, we need to register the application's plugins
    // with the runner in order for them to work on the background isolate. `registerPlugins` is
    // a callback set from AppDelegate.m in the main application. This callback should register
    // all relevant plugins (excluding those which require UI).
    registerPlugins(_headlessRunner);
    [_registrar addMethodCallDelegate:self channel:_callbackChannel];
}

- (FlutterMethodChannel *)channel {
    return _mainChannel;
}

- (NSURLSession*)currentSession {
    return _session;
}

- (void)initializeHlsSessionIfNeeded {
    if (_hlsSession != nil) return;

    NSString *identifier = [NSString stringWithFormat:@"%@.download.hls.background.session", NSBundle.mainBundle.bundleIdentifier];
    NSURLSessionConfiguration *configuration = [NSURLSessionConfiguration backgroundSessionConfigurationWithIdentifier:identifier];
    configuration.discretionary = NO;
    configuration.sessionSendsLaunchEvents = YES;
    _hlsSession = [AVAssetDownloadURLSession sessionWithConfiguration:configuration
                                                assetDownloadDelegate:self
                                                       delegateQueue:nil];

    __weak typeof(self) weakSelf = self;
    [_hlsSession getAllTasksWithCompletionHandler:^(NSArray<__kindof NSURLSessionTask *> *tasks) {
        for (NSURLSessionTask *task in tasks) {
            NSString *taskId = task.taskDescription;
            if (taskId.length == 0) continue;
            NSDictionary *storedTask = [weakSelf loadTaskWithId:taskId];
            if (storedTask != nil) {
                [_runningTaskById setObject:[NSMutableDictionary dictionaryWithDictionary:storedTask]
                                     forKey:taskId];
            }
        }
    }];
}

- (AVAssetDownloadURLSession *)currentHlsSession {
    [self initializeHlsSessionIfNeeded];
    return _hlsSession;
}

- (BOOL)isHlsTaskDictionary:(NSDictionary *)task {
    return [task[KEY_CONTENT_ID] length] > 0;
}

- (NSString *)relativeHlsLocalFilePath:(NSString *)path {
    // AVAssetDownloadURLSession can return path components containing literal
    // percent escapes (for example "Episode%2012"). Preserve them exactly:
    // decoding here changes the on-disk filename and makes the movpkg vanish.
    if ([path hasPrefix:@"Library/"]) return path;
    if ([path hasPrefix:@"/Library/"]) {
        return [path substringFromIndex:1];
    }

    NSRange libraryRange = [path rangeOfString:@"/Library/"];
    if (libraryRange.location == NSNotFound) return path;
    return [path substringFromIndex:libraryRange.location + 1];
}

- (NSURL *)hlsLocalURLForStoredPath:(NSString *)path {
    if (path.length == 0) return nil;

    NSString *relativePath = [self relativeHlsLocalFilePath:path];
    if ([relativePath hasPrefix:@"Library/"]) {
        NSURL *baseURL = [NSURL fileURLWithPath:NSHomeDirectory() isDirectory:YES];
        return [baseURL URLByAppendingPathComponent:relativePath isDirectory:YES];
    }

    return [NSURL fileURLWithPath:path];
}

- (NSArray<NSHTTPCookie *> *)cookiesFromHeaders:(NSDictionary *)headers assetURL:(NSURL *)assetURL {
    NSMutableArray<NSHTTPCookie *> *cookies = [NSMutableArray array];
    NSString *cookieHeader = headers[@"Cookie"] ?: headers[@"cookie"];
    for (NSString *rawCookie in [cookieHeader componentsSeparatedByString:@";"]) {
        NSString *cookieValue = [rawCookie stringByTrimmingCharactersInSet:NSCharacterSet.whitespaceAndNewlineCharacterSet];
        NSRange separator = [cookieValue rangeOfString:@"="];
        if (separator.location == NSNotFound) continue;
        NSString *name = [cookieValue substringToIndex:separator.location];
        NSString *value = [cookieValue substringFromIndex:separator.location + 1];
        if (name.length == 0 || value.length == 0) continue;

        NSDictionary<NSHTTPCookiePropertyKey, id> *properties = @{
            NSHTTPCookieName: name,
            NSHTTPCookieValue: value,
            NSHTTPCookieDomain: assetURL.host ?: @"",
            NSHTTPCookiePath: @"/",
            NSHTTPCookieSecure: @"TRUE",
        };
        NSHTTPCookie *cookie = [NSHTTPCookie cookieWithProperties:properties];
        if (cookie != nil) [cookies addObject:cookie];
    }
    return cookies;
}

- (NSURLSessionDownloadTask*)downloadTaskWithURL: (NSURL*) url fileName: (NSString*) fileName andSavedDir: (NSString*) savedDir andHeaders: (NSString*) headers
{
    NSMutableURLRequest *request = [[NSMutableURLRequest alloc] initWithURL:url];
    if (headers != nil && [headers length] > 0) {
        NSError *jsonError;
        NSData *data = [headers dataUsingEncoding:NSUTF8StringEncoding];
        NSDictionary *json = [NSJSONSerialization JSONObjectWithData:data options:NSJSONReadingMutableContainers error:&jsonError];

        for (NSString *key in json) {
            NSString *value = json[key];
            if (debug) {
                NSLog(@"Header(%@: %@)", key, value);
            }
            [request setValue:value forHTTPHeaderField:key];
        }
    }
    NSURLSessionDownloadTask *task = [[self currentSession] downloadTaskWithRequest:request];
    // store task id in taskDescription
    task.taskDescription = [self createTaskId];
    [task resume];

    return task;
}

- (NSString*) createTaskId {
    return [NSString stringWithFormat:@"%@.download.task.%d.%f",
                            NSBundle.mainBundle.bundleIdentifier, arc4random_uniform(100000), [[NSDate date] timeIntervalSince1970]];
}

- (NSString*)identifierForTask:(NSURLSessionTask*) task
{
    return task.taskDescription;
}

- (NSString*)identifierForTask:(NSURLSessionTask*) task ofSession:(NSURLSession *)session
{
    return task.taskDescription;
}

- (void)updateRunningTaskById:(NSString*)taskId progress:(int)progress status:(int)status resumable:(BOOL)resumable {
    _runningTaskById[taskId][KEY_PROGRESS] = @(progress);
    _runningTaskById[taskId][KEY_STATUS] = @(status);
    _runningTaskById[taskId][KEY_RESUMABLE] = @(resumable);
}

- (void)pauseTaskWithId: (NSString*)taskId
{
    if (debug) {
        NSLog(@"pause task with id: %@", taskId);
    }
    __typeof__(self) __weak weakSelf = self;
    NSDictionary *storedTask = [self loadTaskWithId:taskId];
    if ([self isHlsTaskDictionary:storedTask]) {
        [[self currentHlsSession] getAllTasksWithCompletionHandler:^(NSArray<__kindof NSURLSessionTask *> *tasks) {
            for (NSURLSessionTask *task in tasks) {
                if ([task.taskDescription isEqualToString:taskId] && task.state == NSURLSessionTaskStateRunning) {
                    [task suspend];
                    int progress = [storedTask[KEY_PROGRESS] intValue];
                    [weakSelf updateRunningTaskById:taskId progress:progress status:STATUS_PAUSED resumable:YES];
                    [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_PAUSED) andProgress:@(progress)];
                    [weakSelf executeInDatabaseQueueForTask:^{
                        [weakSelf updateTask:taskId status:STATUS_PAUSED progress:progress resumable:YES];
                    }];
                    return;
                }
            }
        }];
        return;
    }
    [[self currentSession] getTasksWithCompletionHandler:^(NSArray<NSURLSessionDataTask *> *data, NSArray<NSURLSessionUploadTask *> *uploads, NSArray<NSURLSessionDownloadTask *> *downloads) {
        for (NSURLSessionDownloadTask *download in downloads) {
            NSURLSessionTaskState state = download.state;
            NSString *taskIdValue = [weakSelf identifierForTask:download];
            if ([taskId isEqualToString:taskIdValue] && (state == NSURLSessionTaskStateRunning)) {
                NSDictionary *task = [weakSelf loadTaskWithId:taskIdValue];
              
                NSNumber *progressNumOfTask = task[@"progress"];
                int progress = progressNumOfTask.intValue;
                
                [download cancelByProducingResumeData:^(NSData * _Nullable resumeData) {
                    // Save partial downloaded data to a file
                    NSFileManager *fileManager = [NSFileManager defaultManager];
                    NSURL *destinationURL = [weakSelf fileUrlOf:taskId taskInfo:task downloadTask:download];

                    if ([fileManager fileExistsAtPath:[destinationURL path]]) {
                        [fileManager removeItemAtURL:destinationURL error:nil];
                    }

                    BOOL success = [resumeData writeToURL:destinationURL atomically:YES];
                    if (debug) {
                        NSLog(@"save partial downloaded data to a file: %s", success ? "success" : "failure");
                    }
                }];

                [weakSelf updateRunningTaskById:taskId progress:progress status:STATUS_PAUSED resumable:YES];

                [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_PAUSED) andProgress:@(progress)];

                [weakSelf executeInDatabaseQueueForTask:^{
                    [weakSelf updateTask:taskId status:STATUS_PAUSED progress:progress resumable:YES];
                }];
                return;
            }
        };
    }];
}

- (void)cancelTaskWithId: (NSString*)taskId
{
    if (debug) {
        NSLog(@"cancel task with id: %@", taskId);
    }
    __typeof__(self) __weak weakSelf = self;
    NSDictionary *storedTask = [self loadTaskWithId:taskId];
    if ([self isHlsTaskDictionary:storedTask]) {
        [[self currentHlsSession] getAllTasksWithCompletionHandler:^(NSArray<__kindof NSURLSessionTask *> *tasks) {
            for (NSURLSessionTask *task in tasks) {
                if ([task.taskDescription isEqualToString:taskId]) {
                    [task cancel];
                    [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_CANCELED) andProgress:@(-1)];
                    [weakSelf executeInDatabaseQueueForTask:^{
                        [weakSelf updateTask:taskId status:STATUS_CANCELED progress:-1];
                    }];
                    return;
                }
            }
        }];
        return;
    }
    [[self currentSession] getTasksWithCompletionHandler:^(NSArray<NSURLSessionDataTask *> *data, NSArray<NSURLSessionUploadTask *> *uploads, NSArray<NSURLSessionDownloadTask *> *downloads) {
        for (NSURLSessionDownloadTask *download in downloads) {
            NSURLSessionTaskState state = download.state;
            NSString *taskIdValue = [self identifierForTask:download];
            if ([taskId isEqualToString:taskIdValue] && (state == NSURLSessionTaskStateRunning)) {
                [download cancel];
                [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_CANCELED) andProgress:@(-1)];
                [weakSelf executeInDatabaseQueueForTask:^{
                    [weakSelf updateTask:taskId status:STATUS_CANCELED progress:-1];
                }];
                return;
            }
        };
    }];
}

- (void)cancelAllTasks {
    __typeof__(self) __weak weakSelf = self;
    [[self currentSession] getTasksWithCompletionHandler:^(NSArray<NSURLSessionDataTask *> *data, NSArray<NSURLSessionUploadTask *> *uploads, NSArray<NSURLSessionDownloadTask *> *downloads) {
        for (NSURLSessionDownloadTask *download in downloads) {
            NSURLSessionTaskState state = download.state;
            if (state == NSURLSessionTaskStateRunning) {
                [download cancel];
                NSString *taskId = [self identifierForTask:download];
                [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_CANCELED) andProgress:@(-1)];
                [weakSelf executeInDatabaseQueueForTask:^{
                    [weakSelf updateTask:taskId status:STATUS_CANCELED progress:-1];
                }];
            }
        };
    }];
    [[self currentHlsSession] getAllTasksWithCompletionHandler:^(NSArray<__kindof NSURLSessionTask *> *tasks) {
        for (NSURLSessionTask *task in tasks) {
            NSString *taskId = task.taskDescription;
            if (taskId.length == 0) continue;
            [task cancel];
            [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_CANCELED) andProgress:@(-1)];
            [weakSelf executeInDatabaseQueueForTask:^{
                [weakSelf updateTask:taskId status:STATUS_CANCELED progress:-1];
            }];
        }
    }];
}

- (void)sendUpdateProgressForTaskId: (NSString*)taskId inStatus: (NSNumber*) status andProgress: (NSNumber*) progress
{
    NSArray *args = @[@(_callbackHandle), taskId, status, progress];
    if (initialized && _callbackHandle != 0) {
        dispatch_async(dispatch_get_main_queue(), ^{
        [self-> _callbackChannel invokeMethod:@"" arguments:args];
         });
    } else {
        [_eventQueue addObject:args];
    }
}

- (void)executeInDatabaseQueueForTask:(void (^)(void))task {
    __typeof__(self) __weak weakSelf = self;
    dispatch_sync(databaseQueue, ^{
        if (weakSelf.isDatabaseQueueTerminated) return;
        if (task) task();
    });
}

+ (NSArray<NSNumber *> *)avaliableCommonDirectories {
    return @[@(NSCachesDirectory),
             @(NSApplicationSupportDirectory),
             @(NSLibraryDirectory),
             @(kDefaultSearchPathDirectory),
             @(NSDownloadsDirectory)];;
}

- (BOOL)openDocumentWithURL:(NSURL*)url {
    if (debug) {
        NSLog(@"try to open file in url: %@", url);
    }
    BOOL result = NO;
    UIDocumentInteractionController* tmpDocController = [UIDocumentInteractionController
                                                         interactionControllerWithURL:url];
    if (tmpDocController)
    {
        if (debug) {
            NSLog(@"initialize UIDocumentInteractionController successfully");
        }
        tmpDocController.delegate = self;
        result = [tmpDocController presentPreviewAnimated:YES];
    }
    return result;
}

- (NSURL*)fileUrlFromDict:(NSDictionary*)dict
{
    NSString *savedDir = dict[KEY_SAVED_DIR];
    NSString *filename = dict[KEY_FILE_NAME];
    if (debug) {
        NSLog(@"savedDir: %@", savedDir);
        NSLog(@"filename: %@", filename);
    }
    NSURL *savedDirURL = [NSURL fileURLWithPath:savedDir];
    return [savedDirURL URLByAppendingPathComponent:filename];
}

- (NSURL*)fileUrlOf:(NSString*)taskId taskInfo:(NSDictionary*)taskInfo downloadTask:(NSURLSessionDownloadTask*)downloadTask {
     NSString *filename = taskInfo[KEY_FILE_NAME];
     NSString *suggestedFilename = downloadTask.response.suggestedFilename;
     if (debug) {
         NSLog(@"SuggestedFileName: %@", suggestedFilename);
     }
     // Check if filename is nil or empty
     if (filename == nil || ![filename isKindOfClass:[NSString class]] || [filename isEqualToString:@""]) {
         // If suggestedFilename is empty, use the last path component of the URL as the filename
         filename = [self sanitizeFilename:suggestedFilename];
     } 
     // Update the taskInfo with the sanitized filename
     NSMutableDictionary *mutableTaskInfo = [taskInfo mutableCopy];
     mutableTaskInfo[KEY_FILE_NAME] = filename;

     // Update the taskInfo
     if ([_runningTaskById objectForKey:taskId]) {
         _runningTaskById[taskId][KEY_FILE_NAME] = filename;
     }

     // update DB
     __weak typeof(self) weakSelf = self;
     [self executeInDatabaseQueueForTask:^{
         [weakSelf updateTask:taskId filename:filename];
     }];

     return [self fileUrlFromDict:mutableTaskInfo];
}

- (NSString*)absoluteSavedDirPathWithShortSavedDir:(NSString*)shortSavedDir searchPathDirectory:(NSSearchPathDirectory)searchPathDirectory {
    return [[NSSearchPathForDirectoriesInDomains(searchPathDirectory, NSUserDomainMask, YES) firstObject] stringByAppendingPathComponent:shortSavedDir];
}

- (NSString *)sanitizeFilename:(nullable NSString *)filename {
    // Define a list of allowed characters for filenames
    NSMutableCharacterSet *allowedCharacters = [[NSMutableCharacterSet alloc] init];

    // Allow alphabetical characters (lowercase and uppercase)
    [allowedCharacters formUnionWithCharacterSet:[NSCharacterSet letterCharacterSet]];

    // Allow digits
    [allowedCharacters addCharactersInRange:NSMakeRange('0', 10)]; // ASCII digits

    // Allow additional characters: -_.()
    [allowedCharacters addCharactersInString:@"-_.()"];

    // Allow empty spaces
    [allowedCharacters addCharactersInString:@" "];

    // Remove the backslash (if you want to disallow it)
    [allowedCharacters removeCharactersInString:@"\\"];

    // Now, you have a character set that allows the specified characters
    NSCharacterSet *finalCharacterSet = [allowedCharacters copy];
    if (filename == nil || [filename isEqual:[NSNull null]] || [filename isEqualToString:@""]) {
           NSString *defaultFilename = @"default_filename";
           return defaultFilename;
       }
    // Create a mutable string to build the sanitized filename
    NSMutableString *sanitizedFilename = [NSMutableString string];
    
    // Iterate over each character in the original filename
    for (NSUInteger i = 0; i < filename.length; i++) {
        unichar character = [filename characterAtIndex:i];
        
        // Check if the character is in the allowed set
        if ([allowedCharacters characterIsMember:character]) {
            // Append the allowed character to the sanitized filename
            [sanitizedFilename appendFormat:@"%C", character];
        } else {
            // Replace forbidden characters with an underscore
            [sanitizedFilename appendString:@"_"];
        }
    }
    
    // Ensure the sanitized filename is not empty
    if ([sanitizedFilename isEqualToString:@""]) {
        // Provide a default filename if the sanitized one is empty
        NSString *defaultFilename = @"default_filename";
        sanitizedFilename = [[NSMutableString alloc] initWithString:defaultFilename];
    }
    
    return sanitizedFilename;
}



- (NSArray *)shortenSavedDirPath:(NSString*)absolutePath {
    if (debug) {
        NSLog(@"Absolute savedDir path: %@", absolutePath);
    }

    for (NSNumber *element in self.class.avaliableCommonDirectories) {
        NSString *shortSvedDirPath = [self shortenSavedDirPath:absolutePath searchPathDirectory:element.unsignedIntegerValue];
        if (shortSvedDirPath) {
            return @[shortSvedDirPath, element];
        }
    }

    return @[@"", @(kDefaultSearchPathDirectory)];
}

- (NSString*)shortenSavedDirPath:(NSString*)absolutePath searchPathDirectory:(NSSearchPathDirectory)searchPathDirectory {
    if (absolutePath) {
        NSString *searchDirPath = [NSSearchPathForDirectoriesInDomains(searchPathDirectory, NSUserDomainMask, YES) firstObject];
        if ([absolutePath isEqualToString:searchDirPath]) {
            return @"";
        }
        NSRange foundRank = [absolutePath rangeOfString:searchDirPath];
        if (foundRank.length > 0) {
            // we increase the location of range by one because we want to remove the file separator as well.
            NSString *shortenSavedDirPath = [absolutePath substringWithRange:NSMakeRange(foundRank.length + 1, absolutePath.length - searchDirPath.length - 1)];
            return shortenSavedDirPath != nil ? shortenSavedDirPath : @"";
        }
    }
    
    return nil;
}


- (long long)currentTimeInMilliseconds
{
    return (long long)([[NSDate date] timeIntervalSince1970]*1000);
}

# pragma mark - Database Accessing

/// Before version 1.11.1, FlutterDownloader only allows file to be saved in [NSDocumentDirectory]. This limits the freedom of development.
///
/// This function serves two purposes:
///
/// 1. Add a database column `search_dir` for determining common root directory such as the flowing directories
///
///    - NSCachesDirectory
///    - NSApplicationSupportDirectory
///    - NSLibraryDirectory
///    - NSDocumentDirectory
///    - NSDownloadsDirectory
///
///    Definition of common root directory refers to [path_provider](https://github.com/flutter/packages/blob/main/packages/path_provider/path_provider/lib/path_provider.dart).
///
/// 2.  Resolve previous compatibility issue
- (void)addDatabaseColumnForMakingFileCouldSaveInAnyDirectory {
    [_dbManager addLazilyColumnForTable:"task"
                                 column:KEY_SEARCH_DIR.UTF8String
                                   type:"integer"
                           defaultValue:[NSString stringWithFormat:@"%lu", kDefaultSearchPathDirectory].UTF8String]; // kDefaultSearchPathDirectory is [NSDocumentDirectory](9), this is compatible with previous FlutterDownloader versions.
}

- (void)addDatabaseColumnsForHlsDownloads {
    // DBManager's legacy row reader skips SQL NULL values, so new columns use
    // non-null sentinel defaults to preserve record/column alignment.
    [_dbManager addLazilyColumnForTable:"task" column:KEY_CONTENT_ID.UTF8String type:"text" defaultValue:"''"];
    [_dbManager addLazilyColumnForTable:"task" column:KEY_LOCAL_FILE_PATH.UTF8String type:"text" defaultValue:"''"];
}

- (void)updateHlsTaskMetadata:(NSString *)taskId
                    contentId:(NSString *)contentId {
    NSString *query = @"UPDATE task SET content_id = ? WHERE task_id = ?";
    [_dbManager executeQuery:query withParameters:@[contentId ?: @"", taskId]];
}

- (void)updateTask:(NSString *)taskId localFilePath:(NSString *)localFilePath {
    NSString *query = @"UPDATE task SET local_file_path = ? WHERE task_id = ?";
    [_dbManager executeQuery:query withParameters:@[localFilePath ?: @"", taskId]];
}

- (NSString*) escape:(NSString*) origin revert:(BOOL)revert
{
    if ( origin == (NSString *)[NSNull null] )
    {
        return @"";
    }
    return revert
    ? [origin stringByRemovingPercentEncoding]
    : [origin stringByAddingPercentEncodingWithAllowedCharacters:NSCharacterSet.URLQueryAllowedCharacterSet];
}


- (void)addNewTask:(NSString *)taskId
               url:(NSString *)url
            status:(int)status
           progress:(int)progress
           filename:(NSString *)filename
           savedDir:(NSString *)savedDir
           searchDir:(NSSearchPathDirectory)searchDir
           headers:(NSString *)headers
           resumable:(BOOL)resumable
           showNotification:(BOOL)showNotification
           openFileFromNotification:(BOOL)openFileFromNotification {

    headers = [self escape:headers revert:NO];
    
    NSString *query = @"INSERT INTO task (task_id, url, status, progress, file_name, saved_dir, search_dir, headers, resumable, show_notification, open_file_from_notification, time_created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    NSNumber *searchDirValue = @(searchDir);
    NSString *sanitizedFileName = [self sanitizeFilename:filename];
    NSArray *values = @[taskId, url, @(status), @(progress), sanitizedFileName, savedDir, searchDirValue, headers, @(resumable ? 1:0), @(showNotification ? 1 : 0), @(openFileFromNotification ? 1: 0), @([self currentTimeInMilliseconds])];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
    }
}

- (void) updateTask: (NSString*) taskId status: (int) status progress: (int) progress
{

    NSString *query = @"UPDATE task SET status = ?, progress = ? WHERE task_id = ?";
    
    NSArray *values = @[@(status), @(progress), taskId];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
    }
}



- (void)updateTask:(NSString *)taskId filename:(NSString *)filename {
    NSString *query = @"UPDATE task SET file_name = ? WHERE task_id = ?";
    
    // Create an array to hold the parameter values
    NSArray *values = @[filename, taskId];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
    }
}


- (void)updateTask:(NSString *)taskId
             status:(int)status
           progress:(int)progress
          resumable:(BOOL)resumable {
    
    NSString *query = @"UPDATE task SET status = ?, progress = ?, resumable = ? WHERE task_id = ?";
    
    NSArray *values = @[@(status), @(progress), @(resumable ? 1 : 0), taskId];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
    }
}

- (void)updateTask:(NSString *)currentTaskId
          newTaskId:(NSString *)newTaskId
             status:(int)status
          resumable:(BOOL)resumable {
    
    NSString *query = @"UPDATE task SET task_id = ?, status = ?, resumable = ?, time_created = ? WHERE task_id = ?";
    
    NSArray *values = @[newTaskId, @(status), @(resumable ? 1 : 0), @([self currentTimeInMilliseconds]), currentTaskId];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
    }
}

- (void)updateTask:(NSString *)taskId resumable:(BOOL)resumable {
    NSString *query = @"UPDATE task SET resumable = ? WHERE task_id = ?";
    
    NSArray *values = @[@(resumable ? 1 : 0), taskId];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        NSLog(@"Update \n%@\n\n%@",taskId,query);
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
    }
}

- (void)deleteTask:(NSString *)taskId {
    NSString *query = @"DELETE FROM task WHERE task_id = ?";
    
    NSArray *values = @[taskId];
    
    [_dbManager executeQuery:query withParameters:values];
    
    if (debug) {
        NSLog(@"Delete \n%@\n\n%@",taskId,query);
        if (_dbManager.affectedRows != 0) {
            NSLog(@"Query was executed successfully. Affected rows = %d", _dbManager.affectedRows);
        } else {
            NSLog(@"Could not execute the query.");
        }
        
    }
}

- (NSArray*)loadAllTasks{
    NSString *query = @"SELECT * FROM task";
    NSArray *records = [[NSArray alloc] initWithArray:[_dbManager loadDataFromDB:query withParameters:@[]]];
    if (debug) {
        NSLog(@"Load tasks successfully");
    }
    NSMutableArray *results = [NSMutableArray new];
    for(NSArray *record in records) {
        NSDictionary *task = [self taskDictFromRecordArray:record];
         NSLog(@"Task found in load all tasks \n%@", task);
        if (debug) {
            NSLog(@"%@", task);
        }
        [results addObject:task];
    }
    return results;
}

- (NSArray*)loadTasksWithRawQuery: (NSString*)query
{
    NSArray *records = [[NSArray alloc] initWithArray:[_dbManager loadDataFromDB:query withParameters:@[]]];
    if (debug) {
        NSLog(@"Load tasks successfully");
    }
    NSMutableArray *results = [NSMutableArray new];
    for(NSArray *record in records) {
        [results addObject:[self taskDictFromRecordArray:record]];
    }
    return results;
}

- (NSDictionary *)loadTaskWithId:(NSString *)taskId {
    // Check the task in memory-cache first
    if ([_runningTaskById objectForKey:taskId]) {
        return [_runningTaskById objectForKey:taskId];
    } else {
        NSString *query = @"SELECT * FROM task WHERE task_id = ? ORDER BY id DESC LIMIT 1";
        NSArray *parameters = @[taskId];
        NSArray *records = [[NSArray alloc] initWithArray:[_dbManager loadDataFromDB:query  withParameters:parameters]];
        if (debug) {
            NSLog(@"Load task successfully");
        }
        if (records != nil && [records count] > 0) {
            NSArray *record = [records firstObject];
            NSDictionary *task = [self taskDictFromRecordArray:record];
            // Checking if the task is valid
            if (task.count == 0) {
                return nil;
            }
            if ([task[KEY_STATUS] intValue] < STATUS_COMPLETE) {
                [_runningTaskById setObject:[NSMutableDictionary dictionaryWithDictionary:task] forKey:taskId];
            }
            return task;
        }
        return nil;
    }
}

- (NSDictionary*) taskDictFromRecordArray:(NSArray*)record
{
    // added try-catch to fix issue: https://github.com/fluttercommunity/flutter_downloader/issues/218
    @try {
        NSString *taskId = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"task_id"]];
        int status = [[record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"status"]] intValue];
        int progress = [[record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"progress"]] intValue];
        NSString *url = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"url"]];
        NSString *filename = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"file_name"]];
        NSString *shortSavedDir = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"saved_dir"]];

        NSString *searchDirStr = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:KEY_SEARCH_DIR]];
        int searchDir = [searchDirStr intValue];
        NSNumber *searchDirNum = [NSNumber numberWithInt:searchDir];

        NSString *savedDir = [self absoluteSavedDirPathWithShortSavedDir:shortSavedDir searchPathDirectory:searchDir];
        
        NSString *headers = @"";
        // in certain cases, headers column might not be available and will cause NSRangeException
        @try {
            NSString *rawHeaders = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"headers"]];
            headers = [self escape:rawHeaders revert:true];
        } @catch(NSException *ex) {
            NSLog(@"task headers not found: %@", ex);
        }
        int resumable = [[record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"resumable"]] intValue];
        int showNotification = [[record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"show_notification"]] intValue];
        int openFileFromNotification = [[record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"open_file_from_notification"]] intValue];
        long long timeCreated = [[record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:@"time_created"]] longLongValue];
        NSString *contentId = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:KEY_CONTENT_ID]];
        NSString *localFilePath = [record objectAtIndex:[_dbManager.arrColumnNames indexOfObject:KEY_LOCAL_FILE_PATH]];
        if (contentId.length > 0 && localFilePath.length > 0) {
            localFilePath = [self hlsLocalURLForStoredPath:localFilePath].path ?: localFilePath;
        }

        NSMutableDictionary *result = [NSMutableDictionary dictionaryWithObjectsAndKeys:taskId, KEY_TASK_ID, @(status), KEY_STATUS, @(progress), KEY_PROGRESS, url, KEY_URL, filename, KEY_FILE_NAME, headers, KEY_HEADERS, savedDir, KEY_SAVED_DIR, searchDirNum, KEY_SEARCH_DIR, [NSNumber numberWithBool:(resumable == 1)], KEY_RESUMABLE, [NSNumber numberWithBool:(showNotification == 1)], KEY_SHOW_NOTIFICATION, [NSNumber numberWithBool:(openFileFromNotification == 1)], KEY_OPEN_FILE_FROM_NOTIFICATION, @(timeCreated), KEY_TIME_CREATED, nil];
        result[KEY_CONTENT_ID] = contentId ?: @"";
        result[KEY_LOCAL_FILE_PATH] = localFilePath ?: @"";
        return result;
    } @catch(NSException *exception) {
        NSLog(@"invalid task data: %@", exception);
        return [NSDictionary dictionary];
    }
}

# pragma mark - FlutterDownloader

- (void)initializeMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSArray *arguments = call.arguments;
    debug = [arguments[1] boolValue];
    _dbManager.debug = debug;
    [self startBackgroundIsolate:[arguments[0] longLongValue]];
    result([NSNull null]);
}

- (void)didInitializeDispatcherMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    initialized = YES;
    if (_callbackHandle != 0) { // unqueue if callback handler has been set
        [self unqueueStatusEvents];
    }
    result([NSNull null]);
}

- (void)registerCallbackMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSArray *arguments = call.arguments;
    _callbackHandle = [arguments[0] longLongValue];
    _step = [arguments[1] intValue];
    if (initialized) [self unqueueStatusEvents];
    result([NSNull null]);
}

- (void) unqueueStatusEvents {
    @synchronized (self) {
        // unqueue all pending download status events.
        while ([_eventQueue count] > 0) {
            NSArray* args = _eventQueue[0];
            [_eventQueue removeObjectAtIndex:0];
            [_callbackChannel invokeMethod:@"" arguments:@[@(_callbackHandle), args[1], args[2], args[3]]];
        }
    }
}

- (void)enqueueMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *urlString = call.arguments[KEY_URL];
    
    NSString *savedDirFromSource = call.arguments[KEY_SAVED_DIR];
    NSArray *shortSavedDirArgs = [self shortenSavedDirPath:savedDirFromSource];
    NSString *shortSavedDir = shortSavedDirArgs[0];
    NSNumber *searchDirNum = shortSavedDirArgs[1];
    NSSearchPathDirectory searchDir = searchDirNum.unsignedIntegerValue;
    NSString *savedDir = [self absoluteSavedDirPathWithShortSavedDir:shortSavedDir searchPathDirectory:searchDir];

    NSString *fileName = call.arguments[KEY_FILE_NAME];
    NSString *headers = call.arguments[KEY_HEADERS];
    NSNumber *showNotification = call.arguments[KEY_SHOW_NOTIFICATION];
    NSNumber *openFileFromNotification = call.arguments[KEY_OPEN_FILE_FROM_NOTIFICATION];
    
    NSURLSessionDownloadTask *task = [self downloadTaskWithURL:[NSURL URLWithString:urlString] fileName:fileName andSavedDir:savedDir andHeaders:headers];
    
    NSString *taskId = [self identifierForTask:task];
    
    [_runningTaskById setObject: [NSMutableDictionary dictionaryWithObjectsAndKeys:
                                  urlString, KEY_URL,
                                  fileName, KEY_FILE_NAME,
                                  savedDir, KEY_SAVED_DIR,
                                  searchDirNum, KEY_SEARCH_DIR,
                                  headers, KEY_HEADERS,
                                  showNotification, KEY_SHOW_NOTIFICATION,
                                  openFileFromNotification, KEY_OPEN_FILE_FROM_NOTIFICATION,
                                  @(NO), KEY_RESUMABLE,
                                  @(STATUS_ENQUEUED), KEY_STATUS,
                                  @(0), KEY_PROGRESS, nil]
                         forKey:taskId];
    
    __typeof__(self) __weak weakSelf = self;
    
    [self executeInDatabaseQueueForTask:^{
        [weakSelf addNewTask:taskId url:urlString status:STATUS_ENQUEUED progress:0 filename:fileName savedDir:shortSavedDir searchDir:searchDir headers:headers resumable:NO showNotification: [showNotification boolValue] openFileFromNotification: [openFileFromNotification boolValue]];
    }];
    result(taskId);
    [self sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_ENQUEUED) andProgress:@0];
}

- (void)enqueueHlsMethodCall:(FlutterMethodCall *)call result:(FlutterResult)result {
    NSString *urlString = call.arguments[KEY_URL];
    NSString *contentId = call.arguments[KEY_CONTENT_ID];
    NSString *title = call.arguments[KEY_TITLE];
    NSNumber *qualityHeight = call.arguments[KEY_QUALITY_HEIGHT];
    NSNumber *allowCellular = call.arguments[KEY_ALLOW_CELLULAR];
    NSDictionary *headers = call.arguments[KEY_HEADERS];

    NSURL *url = [NSURL URLWithString:urlString];
    NSLog(@"[HLS_DIAG][native] enqueue request contentId=%@ host=%@ path=%@ quality=%@", contentId, url.host, url.path, qualityHeight);
    if (url == nil || contentId.length == 0) {
        result([FlutterError errorWithCode:@"invalid_hls_request"
                                   message:@"A valid HLS URL and contentId are required"
                                   details:nil]);
        return;
    }

    NSArray<NSHTTPCookie *> *cookies = [self cookiesFromHeaders:headers ?: @{} assetURL:url];
    NSDictionary *assetOptions = @{
        AVURLAssetHTTPCookiesKey: cookies,
        AVURLAssetAllowsCellularAccessKey: allowCellular ?: @YES,
    };
    AVURLAsset *asset = [AVURLAsset URLAssetWithURL:url options:assetOptions];
    AVAssetDownloadConfiguration *configuration = [AVAssetDownloadConfiguration downloadConfigurationWithAsset:asset
                                                                                                            title:title.length > 0 ? title : contentId];
    if (qualityHeight.integerValue > 0) {
        NSPredicate *predicate = [AVAssetVariantQualifier predicateForPresentationHeight:qualityHeight.doubleValue
                                                                            operatorType:NSLessThanOrEqualToPredicateOperatorType];
        AVAssetVariantQualifier *qualifier = [AVAssetVariantQualifier assetVariantQualifierWithPredicate:predicate];
        configuration.primaryContentConfiguration.variantQualifiers = @[qualifier];
    }

    AVAssetDownloadTask *task = [[self currentHlsSession] assetDownloadTaskWithConfiguration:configuration];
    if (task == nil) {
        result([FlutterError errorWithCode:@"hls_task_creation_failed"
                                   message:@"iOS could not create an HLS download task"
                                   details:nil]);
        return;
    }

    NSString *taskId = [self createTaskId];
    task.taskDescription = taskId;
    NSLog(@"[HLS_DIAG][native] enqueue created taskId=%@ contentId=%@", taskId, contentId);

    NSMutableDictionary *taskInfo = [NSMutableDictionary dictionaryWithObjectsAndKeys:
                                     urlString, KEY_URL,
                                     title ?: @"", KEY_FILE_NAME,
                                     @"", KEY_SAVED_DIR,
                                     @(NSLibraryDirectory), KEY_SEARCH_DIR,
                                     @"", KEY_HEADERS,
                                     @NO, KEY_SHOW_NOTIFICATION,
                                     @NO, KEY_OPEN_FILE_FROM_NOTIFICATION,
                                     @YES, KEY_RESUMABLE,
                                     @(STATUS_ENQUEUED), KEY_STATUS,
                                     @0, KEY_PROGRESS,
                                     contentId, KEY_CONTENT_ID,
                                     @"", KEY_LOCAL_FILE_PATH,
                                     nil];
    [_runningTaskById setObject:taskInfo forKey:taskId];

    __weak typeof(self) weakSelf = self;
    [self executeInDatabaseQueueForTask:^{
        [weakSelf addNewTask:taskId
                         url:urlString
                      status:STATUS_ENQUEUED
                    progress:0
                    filename:title ?: @""
                    savedDir:@""
                   searchDir:NSLibraryDirectory
                     headers:@""
                   resumable:YES
            showNotification:NO
      openFileFromNotification:NO];
        [weakSelf updateHlsTaskMetadata:taskId contentId:contentId];
    }];

    [task resume];
    result(taskId);
    [self sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_ENQUEUED) andProgress:@0];
}

- (void)isPlayableOfflineMethodCall:(FlutterMethodCall *)call result:(FlutterResult)result {
    NSString *taskId = call.arguments[KEY_TASK_ID];
    NSDictionary *task = [self loadTaskWithId:taskId];
    NSString *storedPath = task[KEY_LOCAL_FILE_PATH];
    NSURL *localURL = [self hlsLocalURLForStoredPath:storedPath];
    NSString *localFilePath = localURL.path ?: @"";
    if (![self isHlsTaskDictionary:task] || localFilePath.length == 0) {
        NSLog(@"[HLS_DIAG][native] playable taskId=%@ rejected hls=%d path=%@", taskId, [self isHlsTaskDictionary:task], localFilePath);
        result(@NO);
        return;
    }

    if (localURL == nil) {
        result(@NO);
        return;
    }
    AVURLAsset *asset = [AVURLAsset URLAssetWithURL:localURL options:nil];
    BOOL exists = [[NSFileManager defaultManager] fileExistsAtPath:localFilePath];
    BOOL playable = asset.assetCache.isPlayableOffline;
    NSString *persistedPath = [self relativeHlsLocalFilePath:localFilePath];
    if (persistedPath.length > 0) {
        [self updateTask:taskId localFilePath:persistedPath];
    }
    NSLog(@"[HLS_DIAG][native] playable taskId=%@ exists=%d path=%@ result=%d", taskId, exists, localFilePath, playable);
    result(@(playable));
}

- (void)loadTasksMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    __typeof__(self) __weak weakSelf = self;
    [self executeInDatabaseQueueForTask:^{
        NSArray* tasks = [weakSelf loadAllTasks];
        result(tasks);
    }];
}

- (void)loadTasksWithRawQueryMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *query = call.arguments[KEY_QUERY];
    __typeof__(self) __weak weakSelf = self;
    [self executeInDatabaseQueueForTask:^{
        NSArray* tasks = [weakSelf loadTasksWithRawQuery:query];
        result(tasks);
    }];
}

- (void)cancelMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *taskId = call.arguments[KEY_TASK_ID];
    [self cancelTaskWithId:taskId];
    result([NSNull null]);
}

- (void)cancelAllMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    [self cancelAllTasks];
    result([NSNull null]);
}

- (void)pauseMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *taskId = call.arguments[KEY_TASK_ID];
    [self pauseTaskWithId:taskId];
    result([NSNull null]);
}

- (void)resumeMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *taskId = call.arguments[KEY_TASK_ID];
    NSDictionary* taskDict = [self loadTaskWithId:taskId];
    if (taskDict != nil) {
        NSNumber* status = taskDict[KEY_STATUS];
        if ([self isHlsTaskDictionary:taskDict]) {
            if ([status intValue] != STATUS_PAUSED) {
                result([FlutterError errorWithCode:@"invalid_status"
                                           message:@"only paused task can be resumed"
                                           details:nil]);
                return;
            }
            __weak typeof(self) weakSelf = self;
            [[self currentHlsSession] getAllTasksWithCompletionHandler:^(NSArray<__kindof NSURLSessionTask *> *tasks) {
                for (NSURLSessionTask *task in tasks) {
                    if ([task.taskDescription isEqualToString:taskId]) {
                        [task resume];
                        int progress = [taskDict[KEY_PROGRESS] intValue];
                        [weakSelf updateRunningTaskById:taskId progress:progress status:STATUS_RUNNING resumable:YES];
                        [weakSelf executeInDatabaseQueueForTask:^{
                            [weakSelf updateTask:taskId status:STATUS_RUNNING progress:progress resumable:YES];
                        }];
                        [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_RUNNING) andProgress:@(progress)];
                        result(taskId);
                        return;
                    }
                }
                result([FlutterError errorWithCode:@"hls_task_not_restored"
                                           message:@"The native HLS task is no longer available; enqueue it again with fresh authorization"
                                           details:nil]);
            }];
            return;
        }
        if ([status intValue] == STATUS_PAUSED) {
            NSURL *partialFileURL = [self fileUrlFromDict:taskDict];

            if (debug) {
                NSLog(@"Try to load resume data at url: %@", partialFileURL);
            }

            NSData *resumeData = [NSData dataWithContentsOfURL:partialFileURL];

            if (resumeData != nil) {
                NSURLSessionDownloadTask *task = [[self currentSession] downloadTaskWithResumeData:resumeData];
                NSString *newTaskId = [self createTaskId];
                task.taskDescription = newTaskId;
                [task resume];

                // update memory-cache, assign a new taskId for paused task
                NSMutableDictionary *newTask = [NSMutableDictionary dictionaryWithDictionary:taskDict];
                newTask[KEY_STATUS] = @(STATUS_RUNNING);
                newTask[KEY_RESUMABLE] = @(NO);
                [_runningTaskById setObject:newTask forKey:newTaskId];
                [_runningTaskById removeObjectForKey:taskId];

                result(newTaskId);

                __typeof__(self) __weak weakSelf = self;
                [self executeInDatabaseQueueForTask:^{
                    [weakSelf updateTask:taskId newTaskId:newTaskId status:STATUS_RUNNING resumable:NO];
                    NSDictionary *task = [weakSelf loadTaskWithId:newTaskId];
                    NSNumber *progress = task[KEY_PROGRESS];
                    [weakSelf sendUpdateProgressForTaskId:newTaskId inStatus:@(STATUS_RUNNING) andProgress:progress];
                }];
            } else {
                result([FlutterError errorWithCode:@"invalid_data"
                                           message:@"not found resume data, this task cannot be resumed"
                                           details:nil]);
            }
        } else {
            result([FlutterError errorWithCode:@"invalid_status"
                                       message:@"only paused task can be resumed"
                                       details:nil]);
        }
    } else {
        result(ERROR_INVALID_TASK_ID);
    }
}

- (void)retryMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *taskId = call.arguments[KEY_TASK_ID];
    NSDictionary* taskDict = [self loadTaskWithId:taskId];
    if (taskDict != nil) {
        NSNumber* status = taskDict[KEY_STATUS];
        if ([self isHlsTaskDictionary:taskDict]) {
            result([FlutterError errorWithCode:@"hls_reauthorization_required"
                                       message:@"Fetch fresh HLS authorization and call enqueueHls again"
                                       details:nil]);
            return;
        }
        if ([status intValue] == STATUS_FAILED || [status intValue] == STATUS_CANCELED) {
            NSString *urlString = taskDict[KEY_URL];
            NSString *savedDir = taskDict[KEY_SAVED_DIR];
            NSString *fileName = taskDict[KEY_FILE_NAME];
            NSString *headers = taskDict[KEY_HEADERS];

            NSURLSessionDownloadTask *newTask = [self downloadTaskWithURL:[NSURL URLWithString:urlString] fileName:fileName andSavedDir:savedDir andHeaders:headers];
            NSString *newTaskId = [self identifierForTask:newTask];

            // update memory-cache
            NSMutableDictionary *newTaskDict = [NSMutableDictionary dictionaryWithDictionary:taskDict];
            newTaskDict[KEY_STATUS] = @(STATUS_ENQUEUED);
            newTaskDict[KEY_PROGRESS] = @(0);
            newTaskDict[KEY_RESUMABLE] = @(NO);
            [_runningTaskById setObject:newTaskDict forKey:newTaskId];
            [_runningTaskById removeObjectForKey:taskId];

            __typeof__(self) __weak weakSelf = self;
            [self executeInDatabaseQueueForTask:^{
                [weakSelf updateTask:taskId newTaskId:newTaskId status:STATUS_ENQUEUED resumable:NO];
            }];
            result(newTaskId);
            [self sendUpdateProgressForTaskId:newTaskId inStatus:@(STATUS_ENQUEUED) andProgress:@(0)];
        } else {
            result([FlutterError errorWithCode:@"invalid_status"
                                       message:@"only failed and canceled task can be retried"
                                       details:nil]);
        }
    } else {
        result(ERROR_INVALID_TASK_ID);
    }
}

- (void)openMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    NSString *taskId = call.arguments[KEY_TASK_ID];
    NSDictionary* taskDict = [self loadTaskWithId:taskId];
    if (taskDict != nil) {
        NSNumber* status = taskDict[KEY_STATUS];
        if ([status intValue] == STATUS_COMPLETE) {
            NSURL *downloadedFileURL = [self fileUrlFromDict:taskDict];

            BOOL success = [self openDocumentWithURL:downloadedFileURL];
            result([NSNumber numberWithBool:success]);
        } else {
            result([FlutterError errorWithCode:@"invalid_status"
                                       message:@"only success task can be opened"
                                       details:nil]);
        }
    } else {
        result(ERROR_INVALID_TASK_ID);
    }
}

- (void)removeMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    __typeof__(self) __weak weakSelf = self;

    NSString *taskId = call.arguments[KEY_TASK_ID];
    Boolean shouldDeleteContent = [call.arguments[@"should_delete_content"] boolValue];
    NSDictionary* taskDict = [self loadTaskWithId:taskId];
    if (taskDict != nil) {
        if ([self isHlsTaskDictionary:taskDict]) {
            [[self currentHlsSession] getAllTasksWithCompletionHandler:^(NSArray<__kindof NSURLSessionTask *> *tasks) {
                for (NSURLSessionTask *task in tasks) {
                    if ([task.taskDescription isEqualToString:taskId]) {
                        [task cancel];
                        break;
                    }
                }
            }];
            [_runningTaskById removeObjectForKey:taskId];
            if (shouldDeleteContent) {
                NSString *localFilePath = taskDict[KEY_LOCAL_FILE_PATH];
                NSURL *localURL = [self hlsLocalURLForStoredPath:localFilePath];
                if (localURL != nil) {
                    [[NSFileManager defaultManager] removeItemAtURL:localURL error:nil];
                }
            }
            [self executeInDatabaseQueueForTask:^{
                [weakSelf deleteTask:taskId];
            }];
            result([NSNull null]);
            return;
        }
        NSNumber* status = taskDict[KEY_STATUS];
        if ([status intValue] == STATUS_ENQUEUED || [status intValue] == STATUS_RUNNING) {
            [[self currentSession] getTasksWithCompletionHandler:^(NSArray<NSURLSessionDataTask *> *data, NSArray<NSURLSessionUploadTask *> *uploads, NSArray<NSURLSessionDownloadTask *> *downloads) {
                for (NSURLSessionDownloadTask *download in downloads) {
                    NSURLSessionTaskState state = download.state;
                    NSString *taskIdValue = [weakSelf identifierForTask:download];
                    if ([taskId isEqualToString:taskIdValue] && (state == NSURLSessionTaskStateRunning)) {
                        [download cancel];
                        [weakSelf sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_CANCELED) andProgress:@(-1)];
                        [weakSelf executeInDatabaseQueueForTask:^{
                            [weakSelf deleteTask:taskId];
                        }];
                        return;
                    }
                };
            }];
        }
        
        [self executeInDatabaseQueueForTask:^{
            [weakSelf deleteTask:taskId];
        }];
        
        if (shouldDeleteContent) {
            NSURL *destinationURL = [self fileUrlFromDict:taskDict];

            NSError *error;
            NSFileManager *fileManager = [NSFileManager defaultManager];

            if ([fileManager fileExistsAtPath:[destinationURL path]]) {
                [fileManager removeItemAtURL:destinationURL error:&error];
                if (debug) {
                    if (error == nil) {
                        NSLog(@"delete content file successfully");
                    } else {
                        NSLog(@"cannot delete content file: %@", [error localizedDescription]);
                    }
                }
            }
        }
        result([NSNull null]);
    } else {
        result(ERROR_INVALID_TASK_ID);
    }
}

# pragma mark - FlutterPlugin

+ (void)registerWithRegistrar:(NSObject<FlutterPluginRegistrar>*)registrar {
    [registrar addApplicationDelegate: [[FlutterDownloaderPlugin alloc] init:registrar]];
}

+ (void)setPluginRegistrantCallback:(FlutterPluginRegistrantCallback)callback {
  registerPlugins = callback;
}

- (void)handleMethodCall:(FlutterMethodCall*)call result:(FlutterResult)result {
    if ([@"initialize" isEqualToString:call.method]) {
        [self initializeMethodCall:call result:result];
    } else if ([@"didInitializeDispatcher" isEqualToString:call.method]) {
        [self didInitializeDispatcherMethodCall:call result:result];
    } else if ([@"registerCallback" isEqualToString:call.method]) {
        [self registerCallbackMethodCall:call result:result];
    } else if ([@"enqueue" isEqualToString:call.method]) {
        [self enqueueMethodCall:call result:result];
    } else if ([@"enqueueHls" isEqualToString:call.method]) {
        [self enqueueHlsMethodCall:call result:result];
    } else if ([@"loadTasks" isEqualToString:call.method]) {
        [self loadTasksMethodCall:call result:result];
    } else if ([@"loadTasksWithRawQuery" isEqualToString:call.method]) {
        [self loadTasksWithRawQueryMethodCall:call result:result];
    } else if ([@"cancel" isEqualToString:call.method]) {
        [self cancelMethodCall:call result:result];
    } else if ([@"cancelAll" isEqualToString:call.method]) {
        [self cancelAllMethodCall:call result:result];
    } else if ([@"pause" isEqualToString:call.method]) {
        [self pauseMethodCall:call result:result];
    } else if ([@"resume" isEqualToString:call.method]) {
        [self resumeMethodCall:call result:result];
    } else if ([@"retry" isEqualToString:call.method]) {
        [self retryMethodCall:call result:result];
    } else if ([@"open" isEqualToString:call.method]) {
        [self openMethodCall:call result:result];
    } else if ([@"remove" isEqualToString:call.method]) {
        [self removeMethodCall:call result:result];
    } else if ([@"isPlayableOffline" isEqualToString:call.method]) {
        [self isPlayableOfflineMethodCall:call result:result];
    } else {
        result(FlutterMethodNotImplemented);
    }
}

- (BOOL)application:(UIApplication *)application handleEventsForBackgroundURLSession:(NSString *)identifier completionHandler:(void (^)(void))completionHandler {
    @synchronized (_backgroundCompletionHandlers) {
        _backgroundCompletionHandlers[identifier] = [completionHandler copy];
    }
    if ([identifier hasSuffix:@".download.hls.background.session"]) {
        [self initializeHlsSessionIfNeeded];
    }
    return YES;
}

# pragma mark - NSURLSessionTaskDelegate
- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didWriteData:(int64_t)bytesWritten totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite
{
    if (totalBytesExpectedToWrite == NSURLSessionTransferSizeUnknown) {
        if (debug) {
            NSLog(@"Unknown transfer size");
        }
    } else {
        NSString *taskId = [self identifierForTask:downloadTask];
        int progress = round(totalBytesWritten * 100 / (double)totalBytesExpectedToWrite);
        NSNumber *lastProgress = _runningTaskById[taskId][KEY_PROGRESS];
        if (([lastProgress intValue] == 0 || (progress > ([lastProgress intValue] + _step)) || progress == 100) && progress != [lastProgress intValue]) {
            
            NSNumber *status;
            if (downloadTask.state == NSURLSessionTaskStateRunning) {
                status = @(STATUS_RUNNING);
            } else {
                NSDictionary *taskDict = [self loadTaskWithId:taskId];
                status = taskDict[@"status"];
            }
            
            [self sendUpdateProgressForTaskId:taskId inStatus:status andProgress:@(progress)];
            __typeof__(self) __weak weakSelf = self;
            [self executeInDatabaseQueueForTask:^{
                [weakSelf updateTask:taskId status:status.intValue progress:progress];
            }];
            _runningTaskById[taskId][KEY_PROGRESS] = @(progress);
        }
    }
}

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didFinishDownloadingToURL:(NSURL *)location
{
    NSString *resolvedTaskId = [self identifierForTask:downloadTask ofSession:session];
    NSDictionary *resolvedTask = [self loadTaskWithId:resolvedTaskId];
    if ([self isHlsTaskDictionary:resolvedTask]) return;
    
    NSHTTPURLResponse *httpResponse = (NSHTTPURLResponse *) downloadTask.response;
    long httpStatusCode = (long)[httpResponse statusCode];
    
    if (debug) {
        NSLog(@"%s HTTP status code: %ld", __FUNCTION__, httpStatusCode);
    }
    
    bool isSuccess = (httpStatusCode >= 200 && httpStatusCode < 300);
    
    if (isSuccess) {
        NSString *taskId = [self identifierForTask:downloadTask ofSession:session];
        NSDictionary *task = [self loadTaskWithId:taskId];
        NSURL *destinationURL = [self fileUrlOf:taskId taskInfo:task downloadTask:downloadTask];
        
        [_runningTaskById removeObjectForKey:taskId];
        
        NSError *error;
        NSFileManager *fileManager = [NSFileManager defaultManager];
        
        // Ensure the destination directory exists
        NSURL *destinationDirectory = [destinationURL URLByDeletingLastPathComponent];
        [fileManager createDirectoryAtURL:destinationDirectory withIntermediateDirectories:YES attributes:nil error:nil];
        
        // Remove the existing file if it exists
        if ([fileManager fileExistsAtPath:[destinationURL path]]) {
            [fileManager removeItemAtURL:destinationURL error:nil];
        }
        
        BOOL success = [fileManager copyItemAtURL:location
                                            toURL:destinationURL
                                            error:&error];
        
        __typeof__(self) __weak weakSelf = self;
        if (success) {
            [self sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_COMPLETE) andProgress:@100];
            [self executeInDatabaseQueueForTask:^{
                [weakSelf updateTask:taskId status:STATUS_COMPLETE progress:100];
            }];
        } else {
            if (debug) {
                NSLog(@"Unable to copy temp file. Error: %@", [error localizedDescription]);
            }
            [self sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_FAILED) andProgress:@(-1)];
            [self executeInDatabaseQueueForTask:^{
                [weakSelf updateTask:taskId status:STATUS_FAILED progress:-1];
            }];
        }
    }
}

# pragma mark - AVAssetDownloadDelegate

- (void)URLSession:(NSURLSession *)session
 assetDownloadTask:(AVAssetDownloadTask *)assetDownloadTask
 willDownloadToURL:(NSURL *)location {
    NSString *taskId = assetDownloadTask.taskDescription;
    if (taskId.length == 0) return;
    NSLog(@"[HLS_DIAG][native] destination raw taskId=%@ absolute=%@ relative=%@ base=%@ home=%@",
          taskId,
          location.absoluteString,
          location.relativePath,
          location.baseURL.absoluteString,
          NSHomeDirectory());
    NSString *persistedPath = [self relativeHlsLocalFilePath:location.relativePath ?: @""];
    NSURL *localURL = [self hlsLocalURLForStoredPath:persistedPath];
    NSString *localFilePath = localURL.path ?: location.path ?: @"";
    BOOL isDirectory = NO;
    BOOL exists = [[NSFileManager defaultManager] fileExistsAtPath:localFilePath isDirectory:&isDirectory];
    NSLog(@"[HLS_DIAG][native] destination taskId=%@ exists=%d directory=%d path=%@", taskId, exists, isDirectory, localFilePath);
    _runningTaskById[taskId][KEY_LOCAL_FILE_PATH] = localFilePath;
    __weak typeof(self) weakSelf = self;
    [self executeInDatabaseQueueForTask:^{
        [weakSelf updateTask:taskId localFilePath:persistedPath];
    }];
}

- (void)URLSession:(NSURLSession *)session
 assetDownloadTask:(AVAssetDownloadTask *)assetDownloadTask
 didLoadTimeRange:(CMTimeRange)timeRange
 totalTimeRangesLoaded:(NSArray<NSValue *> *)loadedTimeRanges
 timeRangeExpectedToLoad:(CMTimeRange)timeRangeExpectedToLoad {
    Float64 expectedSeconds = CMTimeGetSeconds(timeRangeExpectedToLoad.duration);
    if (!isfinite(expectedSeconds) || expectedSeconds <= 0) return;

    Float64 loadedSeconds = 0;
    for (NSValue *value in loadedTimeRanges) {
        loadedSeconds += CMTimeGetSeconds(value.CMTimeRangeValue.duration);
    }
    int progress = (int)round(MIN(1.0, loadedSeconds / expectedSeconds) * 100.0);
    NSString *taskId = assetDownloadTask.taskDescription;
    if (taskId.length == 0) return;
    int lastProgress = [_runningTaskById[taskId][KEY_PROGRESS] intValue];
    if (progress == lastProgress || (progress < lastProgress + _step && progress != 100)) return;

    [self updateRunningTaskById:taskId progress:progress status:STATUS_RUNNING resumable:YES];
    [self sendUpdateProgressForTaskId:taskId inStatus:@(STATUS_RUNNING) andProgress:@(progress)];
    __weak typeof(self) weakSelf = self;
    [self executeInDatabaseQueueForTask:^{
        [weakSelf updateTask:taskId status:STATUS_RUNNING progress:progress resumable:YES];
    }];
}

- (NSString *)normalizedHlsErrorCode:(NSError *)error {
    if (error.code == NSURLErrorCancelled) return @"canceled";
    if (error.code == NSURLErrorNotConnectedToInternet ||
        error.code == NSURLErrorNetworkConnectionLost ||
        error.code == NSURLErrorTimedOut) return @"network";
    if (error.code == NSFileWriteOutOfSpaceError) return @"insufficient_storage";
    if ([error.domain isEqualToString:AVFoundationErrorDomain]) return @"invalid_or_unsupported_hls";
    return @"native_hls_error";
}

-(void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error
{
    NSString *taskId = [self identifierForTask:task ofSession:session];
    NSDictionary *storedTask = [self loadTaskWithId:taskId];
    if (storedTask == nil) return;

    if ([self isHlsTaskDictionary:storedTask]) {
        int status = STATUS_FAILED;
        int progress = -1;
        NSString *errorCode = @"";
        if (error != nil) {
            // AVAssetDownloadTask explicitly does not support NSURLSessionTask.response.
            errorCode = [self normalizedHlsErrorCode:error];
            status = [errorCode isEqualToString:@"canceled"] ? STATUS_CANCELED : STATUS_FAILED;
        } else {
            NSString *localFilePath = storedTask[KEY_LOCAL_FILE_PATH];
            NSURL *localURL = [self hlsLocalURLForStoredPath:localFilePath];
            localFilePath = localURL.path ?: localFilePath;
            AVURLAsset *asset = localURL != nil ? [AVURLAsset URLAssetWithURL:localURL options:nil] : nil;
            BOOL isDirectory = NO;
            BOOL exists = [[NSFileManager defaultManager] fileExistsAtPath:localFilePath isDirectory:&isDirectory];
            NSLog(@"[HLS_DIAG][native] completion taskId=%@ error=nil exists=%d directory=%d playable=%d path=%@", taskId, exists, isDirectory, asset.assetCache.isPlayableOffline, localFilePath);
            if (asset.assetCache.isPlayableOffline) {
                status = STATUS_COMPLETE;
                progress = 100;
            } else {
                errorCode = @"asset_not_playable_offline";
            }
        }

        if (error != nil) {
            NSLog(@"[HLS_DIAG][native] completion taskId=%@ errorCode=%@ domain=%@ code=%ld message=%@", taskId, errorCode, error.domain, (long)error.code, error.localizedDescription);
        }

        [_runningTaskById removeObjectForKey:taskId];
        [self sendUpdateProgressForTaskId:taskId inStatus:@(status) andProgress:@(progress)];
        __weak typeof(self) weakSelf = self;
        [self executeInDatabaseQueueForTask:^{
            [weakSelf updateTask:taskId status:status progress:progress resumable:NO];
        }];
        return;
    }
    
    NSHTTPURLResponse *httpResponse = (NSHTTPURLResponse *) task.response;
    long httpStatusCode = (long)[httpResponse statusCode];
    
    if (debug) {
        NSLog(@"%s HTTP status code: %ld", __FUNCTION__, httpStatusCode);
    }
    
    bool isSuccess = (httpStatusCode >= 200 && httpStatusCode < 300);
    if (error != nil || !isSuccess) {
        if (debug) {
            NSLog(@"Download completed with error: %@", error != nil ? [error localizedDescription] : @(httpStatusCode));
        }
        NSString *taskId = [self identifierForTask:task ofSession:session];
        NSDictionary *taskInfo = [self loadTaskWithId:taskId];
        NSNumber *resumable = taskInfo[KEY_RESUMABLE];
        if (![resumable boolValue]) {
            int status;
            if (error != nil) {
                status = [error code] == -999 ? STATUS_CANCELED : STATUS_FAILED;
            } else {
                status = STATUS_FAILED;
            }
            [_runningTaskById removeObjectForKey:taskId];
            [self sendUpdateProgressForTaskId:taskId inStatus:@(status) andProgress:@(-1)];
            __typeof__(self) __weak weakSelf = self;
            [self executeInDatabaseQueueForTask:^{
                [weakSelf updateTask:taskId status:status progress:-1];
            }];
        }
    }
}

-(void)URLSessionDidFinishEventsForBackgroundURLSession:(NSURLSession *)session
{
    if (debug) {
        NSLog(@"URLSessionDidFinishEventsForBackgroundURLSession:");
    }
    // Check if all download tasks have been finished.
    [session getTasksWithCompletionHandler:^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
        if ([downloadTasks count] == 0) {
            if (debug) {
                NSLog(@"all download tasks have been finished");
            }

            NSString *identifier = session.configuration.identifier;
            __block void(^completionHandler)(void) = nil;
            @synchronized (self->_backgroundCompletionHandlers) {
                completionHandler = self->_backgroundCompletionHandlers[identifier];
                [self->_backgroundCompletionHandlers removeObjectForKey:identifier];
            }
            if (completionHandler != nil) {

                [[NSOperationQueue mainQueue] addOperationWithBlock:^{
                    // Call the completion handler to tell the system that there are no other background transfers.
                    completionHandler();

                    // Show a local notification when all downloads are over.
                    UILocalNotification *localNotification = [[UILocalNotification alloc] init];
                    localNotification.alertBody = self->_allFilesDownloadedMsg;
                    [[UIApplication sharedApplication] presentLocalNotificationNow:localNotification];
                }];
            }
        }
    }];
}


# pragma mark - UIDocumentInteractionControllerDelegate

- (UIViewController *)documentInteractionControllerViewControllerForPreview:(UIDocumentInteractionController *)controller {
    return [UIApplication sharedApplication].delegate.window.rootViewController;
}

- (void)documentInteractionController:(UIDocumentInteractionController *)controller willBeginSendingToApplication:(NSString *)application
{
    if (debug) {
        NSLog(@"Send the document to app %@  ...", application);
    }
}

- (void)documentInteractionController:(UIDocumentInteractionController *)controller didEndSendingToApplication:(NSString *)application
{
    if (debug) {
        NSLog(@"Finished sending the document to app %@  ...", application);
    }

}

- (void)documentInteractionControllerDidDismissOpenInMenu:(UIDocumentInteractionController *)controller
{
    if (debug) {
        NSLog(@"Finished previewing the document");
    }
}

@end
