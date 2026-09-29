from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
        F.col("customers.name"),
    )
)
