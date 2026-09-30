from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .groupBy(F.col("sessions.id"), F.col("sessions.device"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        (F.max(F.col("web_events.duration_sec")) - F.min(F.col("web_events.duration_sec"))).alias("duration_sec_range"),
        F.sum(F.col("web_events.duration_sec")).alias("total_duration_sec"),
    )
    .orderBy(F.col("duration_sec_range").desc(), F.col("total_duration_sec").desc())
)
