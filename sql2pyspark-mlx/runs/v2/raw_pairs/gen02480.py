from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web"]))
    .groupBy("customer_id")
    .agg(F.sum("discount").alias("total_discount"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("CustomerStats"), F.col("customers.id") == F.col("CustomerStats.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.name"),
        F.col("CustomerStats.total_discount"),
    )
)
