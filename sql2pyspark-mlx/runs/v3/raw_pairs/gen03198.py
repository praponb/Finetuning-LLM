from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(390, 400))
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("min_quantity").desc())
)
