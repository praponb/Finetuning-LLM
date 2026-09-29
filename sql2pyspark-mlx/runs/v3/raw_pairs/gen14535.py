from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("discount") < 0.33)
    .groupBy("customer_id")
    .agg(F.sum("discount").alias("total_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("cs.total_discount"),
    )
)
