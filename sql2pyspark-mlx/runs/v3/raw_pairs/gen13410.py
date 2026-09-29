from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([17, 38]))
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("ss.duration_sec_count"),
    )
    .orderBy(F.col("duration_sec_count").desc())
)
