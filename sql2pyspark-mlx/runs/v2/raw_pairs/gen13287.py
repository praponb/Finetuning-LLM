from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .filter(F.col("S.total_count") > 8)
    .select(
        F.col("s.country"),
        F.col("s.device"),
        F.col("S.total_count"),
        F.col("S.latest_event_time"),
    )
)
