from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") >= 443)
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("S.avg_duration_sec"),
    )
    .orderBy("country", "avg_duration_sec", "id")
    .limit(5)
)
