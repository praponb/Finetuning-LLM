from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("web_events.id"),
        F.col("web_events.page"),
        F.col("web_events.session_id"),
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("customers.city"),
        F.col("customers.name"),
        F.upper(F.col("web_events.event_type")).alias("event_type_upper"),
    )
)
