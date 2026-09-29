from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") >= 46)
    .select("unit_price_bucket", "stddev_unit_price")
)
