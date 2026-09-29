from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.browser"), F.col("ss.avg_duration_sec"))
)
