from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 140)
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("quantity").alias("quantity_count"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .orderBy("unit_price_bucket", F.col("stddev_unit_price").desc())
    .limit(20)
)
