from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("stddev_unit_price").desc(), "unit_price_bucket", "product_id")
    .limit(3)
)
