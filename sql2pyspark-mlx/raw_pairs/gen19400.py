from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.hour(F.col("sessions.started_at")).between(7, 10))
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.country"),
        F.col("sessions.browser"),
        F.col("web_events.event_type"),
        F.col("web_events.page"),
        F.col("web_events.event_time"),
    )
    .orderBy("country", F.col("event_type").desc())
)
