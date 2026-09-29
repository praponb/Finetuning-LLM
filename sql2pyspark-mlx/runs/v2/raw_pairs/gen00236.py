from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
