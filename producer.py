# Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

from __future__ import print_function

import boto
import argparse
import json
import threading
import time
import datetime

from argparse import RawTextHelpFormatter
from boto.kinesis.exceptions import ResourceNotFoundException

import config

# To preclude inclusion of aws keys into this code, you may temporarily add
# your AWS credentials to the file:
#     ~/.boto
# as follows:
#     [Credentials]
#     aws_access_key_id = <your access key>
#     aws_secret_access_key = <your secret key>
#
kinesis = boto.connect_kinesis(config.access_key, config.secret_key)

def get_or_create_stream(stream_name, shard_count):
    stream = None
    try:
        stream = kinesis.describe_stream(stream_name)
        print (json.dumps(stream, sort_keys=True, indent=2,
            separators=(',', ': ')))
    except ResourceNotFoundException as rnfe:
        while (stream is None) or ('ACTIVE' not in stream['StreamDescription']['StreamStatus']):
            if stream is None:
                print ('Could not find ACTIVE stream:{0} trying to create.'.format(
                    stream_name))
                kinesis.create_stream(stream_name, shard_count)
            else:
                print ("Stream status: %s" % stream['StreamDescription']['StreamStatus'])
            time.sleep(1)
            stream = kinesis.describe_stream(stream_name)

    return stream

def sum_posts(kinesis_actors):
    """Sum all posts across an array of KinesisPosters
    """
    total_records = 0
    for actor in kinesis_actors:
        total_records += actor.total_records
    return total_records

class KinesisPoster(threading.Thread):
    """
    The Poster thread that repeatedly posts records to shards in a given Kinesis stream.
    """

    def __init__(self, stream_name, partition_key, msg, poster_time=30, quiet=False,
                 name=None, group=None, args=(), kwargs={}):
        super(KinesisPoster, self).__init__(name=name, group=group,
            args=args, kwargs=kwargs)
        self._pending_records = [msg]
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.quiet = quiet
        self.poster_time = poster_time
        self.total_records = 0

    def add_records(self, records):
        """ Add given records to the Poster's pending records list.
        """
        self._pending_records.extend(records)

    def put_all_records(self):
        """Put all pending records in the Kinesis stream."""
        precs = self._pending_records
        self._pending_records = []
        self.put_records(precs)
        self.total_records += len(precs)
        return len(precs)

    def put_records(self, records):
        """Put the given records in the Kinesis stream."""
        for record in records:
            response = kinesis.put_record(
                stream_name=self.stream_name,
                data=record, partition_key=self.partition_key)

            if self.quiet is False:
                print ("-= put seqNum:", response['SequenceNumber'])

    def run(self):
        #start = datetime.datetime.now()
        #finish = start + datetime.timedelta(seconds=self.poster_time)
        self.put_all_records()

        #while finish > datetime.datetime.now():
        #    records_put = self.put_all_records()

        #    if self.quiet is False:
        #        print(self.partition_key, ' Records Put:', records_put)
        #        print(' Total Records Put:', self.total_records)

def assembling(stream_name, data={}):
    """
    split data into smaller and then post them into different shards
    """
    start_time = datetime.datetime.now()
    records = {}
    threads = []
    poster_time = 30

    if not data['tags']: return

    for tag in data['tags']:

        records[tag['url']] = {}
        records[tag['url']]['tag'] = [tag]
        records[tag['url']]['host'] = data['host']
        records[tag['url']]['status'] = data['status']
        records[tag['url']]['timestamp'] = str(start_time)

    print ("The records : {0}".format(records))

    #poster_count = len(records.keys())

    for record, value in records.items():

        poster = KinesisPoster(
            stream_name=stream_name,
            partition_key=record,  # poster's partition key
            poster_time=poster_time,
            msg = json.dumps(value),
            name=record # thread name
            )
        poster.daemon = True

        threads.append(poster)


    for t in threads:
        t.start()

    for t in threads:
        t.join()

    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()
    total_records = sum_posts(threads)
    print ("  Total Records:", total_records)
    print ("     Total Time:", duration)
    print ("  Records / sec:", total_records / duration)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Create or attach to a Kinesis stream and put records in the stream''',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('stream_name',
        help='''the name of the Kinesis stream to either connect with or create''')
    parser.add_argument('--shard_count', type=int, default=1,
        help='''the number of shards to create in the stream, if creating [default: 1]''')
    parser.add_argument('--partition_key', default='PyKinesisExample',
        help='''the partition key to use when communicating records to the
stream [default: 'PyKinesisExample-##']''')
    parser.add_argument('--poster_count', type=int, default=10,
        help='''the number of poster threads [default: 10]''')
    parser.add_argument('--poster_time', type=int, default=30,
        help='''how many seconds the poster threads should put records into
the stream [default: 30]''')
    parser.add_argument('--quiet', action='store_true', default=False,
        help='''reduce console output to just initialization info''')
    parser.add_argument('--delete_stream', action='store_true', default=False,
        help='''delete the Kinesis stream matching the given stream_name''')
    parser.add_argument('--describe_only', action='store_true', default=False,
        help='''only describe the Kinesis stream matching the given stream_name''')

    threads = []
    args = parser.parse_args()
    if (args.delete_stream):
        # delete the given Kinesis stream name
        kinesis.delete_stream(stream_name=args.stream_name)
    else:
        start_time = datetime.datetime.now()

        if args.describe_only is True:
            # describe the given Kinesis stream name
            stream = kinesis.describe_stream(args.stream_name)
            print (json.dumps(stream, sort_keys=True, indent=2,
                separators=(',', ': ')))
        else:
            stream = get_or_create_stream(args.stream_name, args.shard_count)
            # Create a KinesisPoster thread up to the poster_count value
            for pid in xrange(args.poster_count):
                # create poster name per poster thread
                poster_name = 'shard_poster:%s' % pid
                # create partition key per poster thread
                part_key = args.partition_key + '-' + str(pid)
                poster = KinesisPoster(
                    stream_name=args.stream_name,
                    partition_key=part_key,  # poster's partition key
                    poster_time=args.poster_time,
                    name=poster_name,  # thread name
                    quiet=args.quiet)
                poster.daemon = True
                threads.append(poster)
                print ('starting: ', poster_name)
                poster.start()

            # Wait for all threads to complete
            for t in threads:
                t.join()

        finish_time = datetime.datetime.now()
        duration = (finish_time - start_time).total_seconds()
        total_records = sum_posts(threads)
        print ("-=> Exiting Poster Main <=-")
        print ("  Total Records:", total_records)
        print ("     Total Time:", duration)
        print ("  Records / sec:", total_records / duration)
