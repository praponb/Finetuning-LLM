from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.min("event_time").alias("min_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy("session_id", F.col("avg_duration_sec").desc())
)
