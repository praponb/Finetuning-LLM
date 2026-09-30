from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "left")
    .filter(F.col("web_events_by_session.stddev_duration_sec") >= 62)
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("web_events_by_session.duration_sec_range"),
        F.col("web_events_by_session.stddev_duration_sec"),
    )
    .orderBy(F.col("stddev_duration_sec").desc())
)
