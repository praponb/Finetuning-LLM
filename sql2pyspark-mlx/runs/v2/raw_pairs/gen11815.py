from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
    .orderBy("last_event_time")
)
