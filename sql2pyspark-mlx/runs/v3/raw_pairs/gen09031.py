from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.city").isin(["New York", "Chiang Mai"]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_type"),
        F.col("sessions.device"),
        F.col("customers.segment"),
        F.col("customers.credit_limit"),
        F.coalesce(F.col("web_events.duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)
