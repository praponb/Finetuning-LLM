from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("web_events.session_id").isin([12, 27, 33]) & (F.col("customers.country") == "JP"))
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_type"),
        F.col("sessions.device"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
    )
)
