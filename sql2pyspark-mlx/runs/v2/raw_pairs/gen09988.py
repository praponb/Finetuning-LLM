from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-03-01")
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.max_duration_sec") >= 190)
    .select(
        F.col("s.device"),
        F.col("ss.avg_duration_sec"),
        F.col("ss.max_duration_sec"),
    )
)
