from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") < "2025-01-01")
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("S.duration_sec_range"),
        F.col("S.max_duration_sec"),
    )
    .orderBy("max_duration_sec")
)
