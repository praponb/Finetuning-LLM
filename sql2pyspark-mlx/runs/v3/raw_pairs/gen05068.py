from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "outer")
    .filter((F.col("sessions.started_at") < "2024-12-01") & F.col("sessions.country").isin(["TH", "UK", "JP"]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.page"),
        F.col("sessions.started_at"),
        F.col("customers.city"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
