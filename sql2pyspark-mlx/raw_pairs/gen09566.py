from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("customers.email"),
        F.col("customers.city"),
        F.col("t.min_started_at"),
    )
)
