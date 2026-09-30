from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.shipping_fee") >= 39, "high").when(F.col("o.shipping_fee") >= 14, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.min(F.col("oi.unit_price")).alias("min_unit_price"))
)
