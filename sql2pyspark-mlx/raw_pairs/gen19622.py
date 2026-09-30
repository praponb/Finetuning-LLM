from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("sessions.browser") == "Firefox")
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("sessions.customer_id"),
        F.col("customers.is_active"),
    )
    .orderBy(F.col("browser").desc())
)
