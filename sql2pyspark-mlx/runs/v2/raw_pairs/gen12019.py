from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") < "2023-06-01")
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.avg_duration_sec") > 26)
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("t.avg_duration_sec"),
        F.col("t.duration_sec_count"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
