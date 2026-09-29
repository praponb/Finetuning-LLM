from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_anti")
    .filter(F.col("sessions.country") == "JP")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
    )
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
)
