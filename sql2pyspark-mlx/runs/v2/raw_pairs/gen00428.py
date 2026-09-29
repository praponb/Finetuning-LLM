from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("ss.duration_sec_range"),
        F.col("ss.min_event_time"),
    )
    .orderBy("min_event_time", F.col("duration_sec_range").desc())
)
