from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-03-12")
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.avg_duration_sec") >= 61)
    .select(F.col("s.customer_id"), F.col("s.device"), F.col("ss.avg_duration_sec"))
    .orderBy("avg_duration_sec", "customer_id")
)
