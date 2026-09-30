from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("web_events.session_id").isin([14, 32]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.session_id"),
        F.col("sessions.device"),
        F.col("customers.email"),
        F.col("customers.signup_date"),
    )
    .orderBy("event_time")
)
