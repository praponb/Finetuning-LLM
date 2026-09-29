from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("ss.avg_duration_sec"))
    .orderBy(F.col("id").desc())
)
