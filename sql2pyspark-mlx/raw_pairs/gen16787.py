from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-09-20")
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("t.max_duration_sec"),
    )
)
