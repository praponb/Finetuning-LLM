from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 390, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc())
)
