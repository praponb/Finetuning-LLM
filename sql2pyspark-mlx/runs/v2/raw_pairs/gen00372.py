from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("SessionStats"), F.col("sessions.id") == F.col("SessionStats.session_id"), "inner")
    .filter(F.col("SessionStats.avg_duration_sec") >= 39)
    .select(
        F.col("sessions.country"),
        F.col("sessions.started_at"),
        F.col("SessionStats.avg_duration_sec"),
        F.col("SessionStats.unique_pages"),
    )
)
