from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("o.discount") >= 0.38, "high").when(F.col("o.discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
