from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([21, 36]) & (F.col("unit_price") < 240))
    .withColumn("unit_price_band", F.when(F.col("unit_price") >= 380, "High").when(F.col("unit_price") >= 160, "Medium").otherwise("Low"))
)

summary = (
    filtered_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("stddev_quantity") >= 66)
    .orderBy(F.col("avg_unit_price").desc(), "product_id", "quantity_bucket")
    .limit(10)
)
