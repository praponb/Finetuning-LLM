from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") >= "2023-08-01")
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("ss.min_duration_sec"),
    )
    .orderBy(F.col("started_at").desc(), "browser")
)
