from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "inner")
    .filter(F.col("session_stats.duration_sec_count") > 9)
    .select(
        F.col("sessions.started_at"),
        F.col("session_stats.duration_sec_count"),
        F.col("session_stats.latest_event_time"),
    )
    .orderBy("started_at", F.col("duration_sec_count").desc())
)
