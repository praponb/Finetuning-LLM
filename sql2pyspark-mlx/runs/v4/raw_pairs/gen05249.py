from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "right")
    .filter(F.col("sessions.customer_id").isin([21, 26, 30, 34]))
    .select(
        F.col("sessions.country"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
    )
)
