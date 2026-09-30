from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-07-01")
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.total_duration_sec") > 1100)
    .select(F.col("s.id"), F.col("s.browser"), F.col("t.total_duration_sec"))
    .orderBy(F.col("browser").desc())
)
