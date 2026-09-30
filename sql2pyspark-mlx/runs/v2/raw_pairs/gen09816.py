from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("quantity").isNotNull())

order_items_stats = (
    valid_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    order_items_stats
    .filter(F.col("stddev_unit_price") > 66)
    .orderBy(F.col("stddev_unit_price").desc(), "unit_price_bucket", "product_id")
    .limit(10)
)
