from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.store_id").isin([7, 32, 38]))
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 36, "high").when(F.col("o.shipping_fee") >= 5, "mid").otherwise("low").alias("shipping_fee_bucket"),
        F.col("o.customer_id"),
    )
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
)
