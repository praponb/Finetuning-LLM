from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
