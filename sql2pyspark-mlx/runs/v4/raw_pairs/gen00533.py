from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([10, 23, 25, 31]) | (F.col("quantity") <= 4))
    .rollup(
        F.when(F.col("unit_price") >= 300, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
