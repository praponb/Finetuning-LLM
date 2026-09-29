from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy("session_id", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("event_month")
)
