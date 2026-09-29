from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("customer_id") == 34)
    .groupBy("customer_id")
    .agg(F.max("started_at").alias("latest_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("ct.latest_started_at"),
    )
)
