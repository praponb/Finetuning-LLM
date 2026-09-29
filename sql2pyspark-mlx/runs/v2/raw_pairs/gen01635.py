from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)
