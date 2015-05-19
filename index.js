/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
	region = "us-east-1";
	console.log("AWS Lambda Redshift Database Loader using default region " + region);
}

var aws = require('aws-sdk');
aws.config.update({
	region : region
});
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : region
});
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : region
});
var sns = new aws.SNS({
	apiVersion : '2010-03-31',
	region : region
});
require('./constants');
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');
var pg = require('pg');
var upgrade = require('./upgrades');
var path = require('path');

// main function for AWS Lambda
exports.handler =
		function(event, context) {
			/** runtime functions * */


			/* callback run when we find a configuration for load in Dynamo DB */
			exports.foundConfig =
					function(s3Info, err, data) {
						if (err) {
							console.log(err);
							var msg = 'Error getting Redshift Configuration for ' + s3Info.dynamoLookup + ' from Dynamo DB ';
							console.log(msg);
							context.done(error, msg);
						}

						if (!data || !data.Item) {
							// finish with no exception - where this file sits
							// in the S3
							// structure is not configured for redshift loads
							console.log("No Configuration Found for " + s3Info.dynamoLookup);

							context.done(null, null);
						} else {
							console.log("Found Redshift Load Configuration for " + s3Info.dynamoLookup);
							var config = data.Item;
							var thisBatchId = config.currentBatch.S;
							exports.createManifest(config, thisBatchId, s3Info, s3Info);
						}
					};



			/**
			 * Function which will create the manifest for a given batch and entries
			 */
			exports.createManifest =
					function(config, thisBatchId, s3Info, batchEntry) {
						console.log("Creating Manifest for Batch " + thisBatchId);

						var manifestInfo = common.createManifestInfo(config);

						// create the manifest file for the file to be loaded
						var manifestContents = {
							entries : []
						};

							manifestContents.entries.push({
								/*
								 * fix url encoding for files with spaces. Space values come in
								 * from Lambda with '+' and plus values come in as %2B. Redshift
								 * wants the original S3 value
								 */
								url : 's3://' + r.s3.bucket.name + "/" +  batchEntry.fullPath.replace('+', ' ').replace('%2B', '+'),
								mandatory : true
							});

						var s3PutParams = {
							Bucket : manifestInfo.manifestBucket,
							Key : manifestInfo.manifestPrefix,
							Body : JSON.stringify(manifestContents)
						};

						console.log("Writing manifest to " + manifestInfo.manifestBucket + "/" + manifestInfo.manifestPrefix);
						/*
						 * save the manifest file to S3 and build the rest of the copy
						 * command in the callback letting us know that the manifest was
						 * created correctly
						 */
						s3.putObject(s3PutParams, exports.loadRedshiftWithManifest.bind(undefined, config, thisBatchId, s3Info,
								manifestInfo));
					};

			/**
			 * Function run when the Redshift manifest write completes succesfully
			 */
			exports.loadRedshiftWithManifest = function(config, thisBatchId, s3Info, manifestInfo, err, data) {
				if (err) {
					console.log("Error on Manifest Creation");
					console.log(err);
					exports.failBatch(err, config, thisBatchId, s3Info, manifestInfo);
				} else {
					console.log("Created Manifest " + manifestInfo.manifestPath + " Successfully");

					// add the manifest file to the batch - this will NOT stop
					// processing if it fails

					// convert the config.loadClusters list into a format that
					// looks like a native dynamo entry
					clustersToLoad = [];
					for (var i = 0; i < config.loadClusters.L.length; i++) {
						clustersToLoad[clustersToLoad.length] = config.loadClusters.L[i].M;
					}

					console.log("Loading " + clustersToLoad.length + " Clusters");

					// run all the cluster loaders in parallel
					async.map(clustersToLoad, function(item, callback) {
						// call the load cluster function, passing it the
						// continuation callback
						exports.loadCluster(config, thisBatchId, s3Info, manifestInfo, item, callback);
					}, function(err, results) {
						if (err) {
							console.log(err);
						}

						// go through all the results - if they were all OK,
						// then close the batch OK - otherwise fail
						var allOK = true;
						var loadState = {};

						for (var i = 0; i < results.length; i++) {
							if (!results[i] || results[i].status === ERROR) {
								allOK = false;
								console.log("Cluster Load Failure " + results[i].error + " on Cluster " + results[i].cluster);
							} 
							// log the response state for each cluster
							loadState[results[i].cluster] = {
								status : results[i].status,
								error : results[i].error
							};
						}

						if (allOK === true) {
							// close the batch as OK
							context.done(null,null);
						} else {
							// close the batch as failure
							exports.failBatch(loadState, config, thisBatchId, s3Info, manifestInfo, loadState);
						}
					});
				}
			};

			/**
			 * Function which loads a redshift cluster
			 * 
			 */
			exports.loadCluster =
					function(config, thisBatchId, s3Info, manifestInfo, clusterInfo, callback) {
						/* build the redshift copy command */
						var copyCommand = '';

						// add the truncate option if requested
						if (clusterInfo.truncateTarget && clusterInfo.truncateTarget.BOOL) {
							copyCommand = 'truncate table ' + clusterInfo.targetTable.S + ';\n';
						}

						var encryptedItems =
								[ kmsCrypto.stringToBuffer(config.secretKeyForS3.S),
										kmsCrypto.stringToBuffer(clusterInfo.connectPassword.S) ];

						// decrypt the encrypted items
						kmsCrypto.decryptAll(encryptedItems, function(err, decryptedConfigItems) {
							if (err) {
								callback(err, {
									status : ERROR,
									cluster : clusterInfo.clusterEndpoint.S
								});
							} else {
								console.log("Full Path: " + s3Info.fullPath);
								copyCommand = copyCommand + 'begin;\nCOPY ' + path.basename(s3Info.fullPath,".csv") + ' from \'s3://'
								+ manifestInfo.manifestPath + '\' with credentials as \'aws_access_key_id='
								+ config.accessKeyForS3.S + ';aws_secret_access_key=' + decryptedConfigItems[0].toString()
								+ '\' manifest ';

								// add data formatting directives
								if (config.dataFormat.S === 'CSV') {
									copyCommand = copyCommand + ' delimiter \'' + config.csvDelimiter.S + '\'\n';
								} else if (config.dataFormat.S === 'JSON') {
									if (config.jsonPath !== undefined) {
										copyCommand = copyCommand + 'json \'' + config.jsonPath.S + '\'\n';
									} else {
										copyCommand = copyCommand + 'json \'auto\' \n';
									}
								} else {
									callback(null, {
										status : ERROR,
										error : 'Unsupported data format ' + config.dataFormat.S,
										cluster : clusterInfo.clusterEndpoint.S
									});
								}

								// add compression directives
								if (config.compression !== undefined) {
									copyCommand = copyCommand + ' ' + config.compression.S + '\n';
								}

								// add copy options
								if (config.copyOptions !== undefined) {
									copyCommand = copyCommand + config.copyOptions.S + '\n';
								}

								copyCommand = copyCommand + ";\ncommit;";
								console.log(copyCommand);
								// build the connection string
								var dbString =
										'postgres://' + clusterInfo.connectUser.S + ":" + decryptedConfigItems[1].toString() + "@"
												+ clusterInfo.clusterEndpoint.S + ":" + clusterInfo.clusterPort.N;
								if (clusterInfo.clusterDB) {
									dbString = dbString + '/' +  s3Info.prefix.split(path.sep)[1];
								}
								console.log("Connecting to Database " + clusterInfo.clusterEndpoint.S + ":" + clusterInfo.clusterPort.N);

								/*
								 * connect to database and run the copy command set
								 */
								pg.connect(dbString, function(err, client, done) {
									if (err) {
										callback(null, {
											status : ERROR,
											error : err,
											cluster : clusterInfo.clusterEndpoint.S
										});
									} else {
										client.query(copyCommand, function(err, result) {
											// release the client thread back to
											// the pool
											done();

											// handle errors and cleanup
											if (err) {
												callback(null, {
													status : ERROR,
													error : err,
													cluster : clusterInfo.clusterEndpoint.S
												});
											} else {
												console.log("Load Complete");

												callback(null, {
													status : OK,
													error : null,
													cluster : clusterInfo.clusterEndpoint.S
												});
											}
										});
									}
								});
							}
						});
					};

			/**
			 * Function which marks a batch as failed and sends notifications
			 * accordingly
			 */
			exports.failBatch =
					function(loadState, config, thisBatchId, s3Info, manifestInfo) {
						if (config.failedManifestKey && manifestInfo) {
							// copy the manifest to the failed location
							manifestInfo.failedManifestPrefix =
									manifestInfo.manifestPrefix.replace(manifestInfo.manifestKey + '/', config.failedManifestKey.S + '/');
							manifestInfo.failedManifestPath = manifestInfo.manifestBucket + '/' + manifestInfo.failedManifestPrefix;

							var copySpec = {
								Bucket : manifestInfo.manifestBucket,
								Key : manifestInfo.failedManifestPrefix,
								CopySource : manifestInfo.manifestPath
							};
							s3.copyObject(copySpec, function(err, data) {
								if (err) {
									console.log(err);
								} else {
									console.log('Created new Failed Manifest ' + manifestInfo.failedManifestPath);
								}
							});
						} else {
							console.log('Not requesting copy of Manifest to Failed S3 Location');
						}
					};


			/** send an SNS message to a topic */
			exports.sendSNS = function(topic, subj, msg, successCallback, failureCallback) {
				var m = {
					Message : JSON.stringify(msg),
					Subject : subj,
					TopicArn : topic
				};

				sns.publish(m, function(err, data) {
					if (err) {
						if (failureCallback) {
							failureCallback(err);
						} else {
							console.log(err);
						}
					} else {
						if (successCallback) {
							successCallback();
						}
					}
				});
			};

			/** Send SNS notifications if configured for OK vs Failed status */
			exports.notify =
					function(config, thisBatchId, s3Info, manifestInfo, batchError) {
						var statusMessage = batchError ? 'error' : 'ok';
						var errorMessage = batchError ? JSON.stringify(batchError) : null;
						var messageBody = {
							error : errorMessage,
							status : statusMessage,
							batchId : thisBatchId,
							s3Prefix : s3Info.fullPath
						};

						if (manifestInfo) {
							messageBody.originalManifest = manifestInfo.manifestPath;
							messageBody.failedManifest = manifestInfo.failedManifestPath;
						}

						if (batchError && batchError !== null) {
							console.log(JSON.stringify(batchError));

							if (config.failureTopicARN) {
								exports.sendSNS(config.failureTopicARN.S, "Lambda Redshift Batch Load " + thisBatchId + " Failure",
										messageBody, function() {
											context.done(error, JSON.stringify(batchError));
										}, function(err) {
											console.log(err);
											context.done(error, err);
										});
							} else {
								context.done(error, batchError);
							}
						} else {
							if (config.successTopicARN) {
								exports.sendSNS(config.successTopicARN.S, "Lambda Redshift Batch Load " + thisBatchId + " OK",
										messageBody, function() {
											context.done(null, null);
										}, function(err) {
											console.log(err);
											context.done(error, err);
										});
							} else {
								// finished OK - no SNS notifications for
								// success
								console.log("Batch Load " + thisBatchId + " Complete");
								context.done(null, null);
							}
						}
					};
			/* end of runtime functions */

			// commented out event logger, for debugging if needed
			// console.log(JSON.stringify(event));
					
			if (!event.Records) {
				// filter out unsupported events
				console.log("Event type unsupported by Lambda Redshift Loader");
				console.log(JSON.stringify(event));
				context.done(null, null);
			} else {
				if (event.Records.length > 1) {
					context.done(error, "Unable to process multi-record events");
				} else {
					for (var i = 0; i < event.Records.length; i++) {
						var r = event.Records[i];
						console.log(r.s3.object);
						// ensure that we can process this event based on a variety
						// of criteria
						var noProcessReason = undefined;
						if (r.eventSource !== "aws:s3") {
							noProcessReason = "Invalid Event Source " + r.eventSource;
						}
						if (!(r.eventName === "ObjectCreated:Copy" || r.eventName === "ObjectCreated:Put" || r.eventName === 'ObjectCreated:CompleteMultipartUpload')) {
							noProcessReason = "Invalid Event Name " + r.eventName;
						}
						if (r.s3.s3SchemaVersion !== "1.0") {
							noProcessReason = "Unknown S3 Schema Version " + r.s3.s3SchemaVersion;
						}

						if (noProcessReason) {
							console.log(noProcessReason);
							context.done(error, noProcessReason);
						} else {
							// extract the s3 details from the event
							var inputInfo = {
								bucket : undefined,
								key : undefined,
								prefix : undefined,
								inputFilename : undefined
							};

							inputInfo.bucket = r.s3.bucket.name;
							inputInfo.key = decodeURIComponent(r.s3.object.key);

							// remove the bucket name from the key, if we have
							// received it
							// - happens on object copy
							inputInfo.key = inputInfo.key.replace(inputInfo.bucket + "/", "");

							var keyComponents = inputInfo.key.split('/');
							inputInfo.inputFilename = keyComponents[keyComponents.length - 1];

							// remove the filename from the prefix value
							var searchKey = inputInfo.key.replace(inputInfo.inputFilename, '').replace(/\/$/, '');

							// if the event didn't have a prefix, and is just in the
							// bucket, then just use the bucket name, otherwise add the prefix
							if (searchKey && searchKey !== null && searchKey !== "") {
								var regex = /(=\d+)+/;
								// transform hive style dynamic prefixes into static
								// match prefixes
								do {
									searchKey = searchKey.replace(regex, "=*");
								} while (searchKey.match(regex) !== null);

								searchKey = "/" + searchKey;
							}
							inputInfo.prefix = inputInfo.bucket + searchKey;
							inputInfo.fullPath = r.s3.object.key;

							inputInfo.dynamoLookup = inputInfo.bucket + "/" + keyComponents[0]; //the site		
							// load the configuration for this prefix, which will kick off
							// the callback chain
							var dynamoLookup = {
								Key : {
									s3Prefix : {
										S : inputInfo.dynamoLookup
									}
								},
								TableName : configTable,
								ConsistentRead : true
							};

							var proceed = false;
							var lookupConfigTries = 10;
							var tryNumber = 0;
							var configData = null;

							async.whilst(function() {
								// return OK if the proceed flag has been set, or if
								// we've hit the retry count
								return !proceed && tryNumber < lookupConfigTries;
							}, function(callback) {
								tryNumber++;
								// lookup the configuration item, and run
								// foundConfig on completion
								dynamoDB.getItem(dynamoLookup, function(err, data) {
									if (err) {
										if (err.code === provisionedThroughputExceeded) {
											// sleep for bounded jitter time up to 1
											// second and then retry
											var timeout = common.randomInt(0, 1000);
											console.log(provisionedThroughputExceeded + " while accessing Configuration. Retrying in "
													+ timeout + " ms");
											setTimeout(callback(), timeout);
										} else {
											// some other error - call the error callback
											callback(err);
										}
									} else {
										configData = data;
										proceed = true;
										callback(null);
									}
								});
							}, function(err) {
								if (err) {
									// fail the context as we haven't been able to
									// lookup the onfiguration
									console.log(err);

									context.done(error, err);
								} else {
									// call the foundConfig method with the data item
									exports.foundConfig(inputInfo, null, configData);
								}
							});
						}
					}
				}
			}
		};