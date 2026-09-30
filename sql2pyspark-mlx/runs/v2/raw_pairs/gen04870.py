from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)
