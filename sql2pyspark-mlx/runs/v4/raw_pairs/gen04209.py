from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("stddev_duration_sec") < 21)
)
