from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 13, 24, 40]))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("event_time").alias("min_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy("avg_duration_sec")
)
