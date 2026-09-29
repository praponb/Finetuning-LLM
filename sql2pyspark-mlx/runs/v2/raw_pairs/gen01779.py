from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("customer_id").isin([9, 12]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("discount")).otherwise(0)).alias("partner_discount"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.avg_shipping_fee") >= 75)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("cs.partner_discount"),
        F.col("cs.avg_shipping_fee"),
    )
)
