from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
        F.sum(F.when(F.col("o.status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
)
