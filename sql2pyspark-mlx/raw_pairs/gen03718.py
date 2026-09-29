from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "outer")
    .groupBy(
        F.when(F.col("o.discount") >= 0.39, "high").when(F.col("o.discount") >= 0.19, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 51)
    .orderBy(F.col("avg_unit_price").desc())
)
