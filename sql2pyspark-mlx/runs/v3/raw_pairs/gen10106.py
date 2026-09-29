from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.min("event_time").alias("first_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy("page", "latest_event_time")
)
