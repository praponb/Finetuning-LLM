from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.started_at"), F.col("t.duration_sec_count"))
)
