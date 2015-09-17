var aws = require ( 'aws-sdk' );
var R = require ( 'ramda' );
var hl = require ( 'highland');
var K = new aws.Kinesis()

var timeouts = [ 0, 2000 ];
var debug = false;

var log = function(message) {
    var inspect;

    if (debug) {
        inspect = require('eyes').inspector({maxLength: 0});
        ({'Array': inspect, 'Object': inspect}[R.type(message)] || console.log)(message);
    }
};

var wrapKinesisFunction = R.curry(function(kinesis, func, params) {
    log('wrapKinesisFunction');

    return hl.wrapCallback(function (params, callBack) {
        kinesis[func](params, callBack);
    })(params);
});

var initialStream = hl();

var fetchData = hl(initialStream)
    .flatMap(function(shardIterator) {
        return hl(function(push, next) {
            K.getRecords({ShardIterator: shardIterator}, function(err, result) {
                push(err, result);
                push(null, hl.nil);
            })
        })
        .errors(function(error, push) {
            log('error');

            if (error.code !== 'ProvisionedThroughputExceededException') {
                log('It\'s a real error');
                return push(error);
            }

            log('It\'s a throughput error');

            return push(null, {NextShardIterator: nextShardIterator, Records: []});
        })
    })
    .map(function(data) {
        initialStream.write(data.NextShardIterator);
        return data.Records;
    })
    .compact()

module.exports = R.curry(function(awsConfig) {
    var kinesis = new aws.Kinesis(R.pick(['accessKeyId', 'secretAccessKey', 'region'], awsConfig));
    var kinesisConfig = R.pick(['StreamName', 'ShardIteratorType'], awsConfig);

    debug = awsConfig.debug || false;

    return wrapKinesisFunction(kinesis, 'describeStream', R.omit(['ShardIteratorType'], kinesisConfig))
        .pluck('StreamDescription')
        .pluck('Shards')
        .map(R.pluck('ShardId'))
        .flatMap(function(shardIdArray) {
            return hl(shardIdArray)
                .map(R.createMapEntry('ShardId'))
                .map(R.merge(kinesisConfig))
                .flatMap(function(shardConfig) {
                    return wrapKinesisFunction(kinesis, 'getShardIterator', shardConfig)
                        .flatMap(function(result) {
                            initialStream.write(result.ShardIterator);
                            return fetchData;
                        });
                })
                .sequence()
                .map(function(record) {
                    return record.Data.toString();
                })
        })
});
