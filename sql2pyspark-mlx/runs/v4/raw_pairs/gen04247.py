from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") > 220)
    .drop("min_unit_price")
    .orderBy("stddev_quantity")
)
