from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy("session_id", "page")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
