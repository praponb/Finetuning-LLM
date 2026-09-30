from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 11)
    .groupBy("page")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .orderBy("page")
)
