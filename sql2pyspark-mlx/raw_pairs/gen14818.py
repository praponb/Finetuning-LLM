from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("S.avg_duration_sec"),
        F.col("S.total_count"),
    )
)
