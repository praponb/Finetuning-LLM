from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .groupBy(F.col("sessions.id"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.round(F.avg(F.col("web_events.duration_sec")), 2).alias("avg_duration_sec"),
    )
    .orderBy("avg_duration_sec", "id")
    .limit(5)
)
