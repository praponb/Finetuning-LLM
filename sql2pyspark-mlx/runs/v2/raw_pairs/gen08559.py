from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("customers.credit_limit"),
        F.col("customers.segment"),
        F.col("customers.name"),
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
    )
)
