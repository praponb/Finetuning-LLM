from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.discount") >= 0.32, "high").when(F.col("o.discount") >= 0.1, "mid").otherwise("low").alias("discount_bucket"),
        F.when(F.col("o.shipping_fee") >= 37, "high").when(F.col("o.shipping_fee") >= 19, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
)
