from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .filter(F.col("S.min_duration_sec") > 150)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("S.min_duration_sec"))
    .orderBy(F.col("min_duration_sec").desc(), "id")
    .limit(5)
)
