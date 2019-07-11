#import the libaries need to read the config file
#start and run the spark session and process the JSON files
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# access and read the config file
config = configparser.ConfigParser()
config.read('dl.cfg')


#access the the AWS key_id and the secret acsess key fromthe config file 
# this willallow the program to read and write to the amazon S3 buckets
print(config.get('USER','AWS_ACCESS_KEY_ID'))
os.environ['AWS_ACCESS_KEY_ID']=config.get('USER','AWS_ACCESS_KEY_ID')#config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('USER','AWS_SECRET_ACCESS_KEY')#config['AWS_SECRET_ACCESS_KEY']


# create the spark session on the EMR cluster
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


#Function to  process the song JSON song data to create the songs and artists tables
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    song_df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(f'{output_data}songs_table', mode='overwrite')

    # extract columns to create artists table
    artists_table = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') 
    artists_table = artists_table.toDF('artist_id', 'name', 'location', 'lattitude','longitude')

    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists_table', mode='overwrite')

#Function to  process the song JSON log data to create the users,time and songplays tables
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    log_df = spark.read.json(log_data).dropDuplicates() 
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select('userId','firstName','lastName','gender','level')  
    users_table = users_table.toDF('user_id','first_name','last_name','gender','level' )  
       
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users_table', mode='overwrite')

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    log_df = log_df.withColumn("datetime", get_datetime(log_df.ts)) 
    
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts)) 
    
    # extract columns to create time table
    time_table =  log_df.select(('timestamp')
                            ,hour('datetime').alias('hour')
                            ,dayofmonth('datetime').alias('day')
                            ,weekofyear('datetime').alias('week')
                            ,month('datetime').alias('month')
                            ,year('datetime').alias('year')
                            ,date_format('datetime','u').alias('weekday')) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(f'{output_data}time_table', mode='overwrite')


    # read in song data to use for songplays table
    song_df = song_df.select('song_id','artist_id','artist_name') 
    
    #join the log and song dataframes 
    songplays_table = log_df.join(song_df,log_df.artist == song_df.artist_name)

    # add an auto incrementing column to the song pay tables
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table.select('songplay_id'
                                             ,'ts'
                                             ,'userId'
                                             ,'level'
                                             ,'song_id'
                                             ,'artist_id'
                                             ,'sessionId'
                                             ,'location'
                                             ,'userAgent'
                                            ,month('datetime').alias('month')
                                            ,year('datetime').alias('year')
                                            )
    # rename the columns in the songplays_tables
    songplays_table.toDF('songplay_id'
                         ,'start_time'
                         ,'user_id'
                         ,'level'
                         ,'song_id'
                         ,'artist_id'
                         ,'session_id'
                         ,'location'
                         ,'user_agent'
                         ,'month'
                         ,'year'
                        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(f'{output_data}songplays_table', mode='overwrite')


# create the spark session and call the functions
# that will process the JSON files to the star shcema
# parquet files.
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://swtown-udacity-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
