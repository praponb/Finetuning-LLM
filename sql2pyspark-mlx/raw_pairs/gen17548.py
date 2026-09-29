from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("customers.age").between(53, 57))
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
    )
)
