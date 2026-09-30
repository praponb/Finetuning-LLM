from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 9, 10]) & (F.col("session_id") == 39))
    .groupBy("page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)
