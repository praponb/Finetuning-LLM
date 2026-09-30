from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("event_type") == "add_to_cart").alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
    )
    .orderBy(F.col("id").desc(), "started_at")
    .limit(1)
)
