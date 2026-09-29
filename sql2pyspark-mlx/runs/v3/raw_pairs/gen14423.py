from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store", "partner"]))
    .groupBy("customer_id")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customer_stats.discount_count"),
    )
    .orderBy("segment")
)
