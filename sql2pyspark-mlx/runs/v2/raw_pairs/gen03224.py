from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.to_date("event_time") >= "2025-04-01")
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.pct_purchase") > 77)
    .select(
        F.col("s.started_at"),
        F.col("ss.max_duration_sec"),
        F.col("ss.pct_purchase"),
    )
)
