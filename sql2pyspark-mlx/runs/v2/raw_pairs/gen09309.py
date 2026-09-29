from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.min("event_time").alias("first_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)
