from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .filter(F.col("stddev_unit_price") > 41)
    .select("unit_price_bucket", "avg_quantity", "stddev_unit_price")
    .orderBy(F.col("avg_quantity").desc())
)
