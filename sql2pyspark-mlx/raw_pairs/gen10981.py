from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .filter(F.col("customer_stats.avg_shipping_fee") > 18)
    .select(
        F.col("customers.id"),
        F.col("customers.country"),
        F.col("customer_stats.avg_shipping_fee"),
    )
)
