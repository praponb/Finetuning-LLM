from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("sessions.customer_id").isin([3, 4, 15, 17]) & F.col("customers.credit_limit").between(5600, 15200))
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("customers.email"),
        F.col("customers.segment"),
        F.col("customers.age"),
        F.ceil(F.col("customers.age")).alias("age_whole"),
    )
)
