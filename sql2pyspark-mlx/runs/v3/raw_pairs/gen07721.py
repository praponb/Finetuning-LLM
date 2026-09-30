from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("sessions.browser") == "Edge") & F.upper(F.col("customers.email")).like("%OB.%"))
    .select(
        F.col("sessions.device"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("customers.signup_date"),
        F.col("customers.city"),
    )
)
