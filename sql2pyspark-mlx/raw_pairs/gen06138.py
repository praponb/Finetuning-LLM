from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec") < 439)
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.max_duration_sec") >= 280)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("ss.max_duration_sec"))
    .orderBy(F.col("id").desc(), "started_at")
)
