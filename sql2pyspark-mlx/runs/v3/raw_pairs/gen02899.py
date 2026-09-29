from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("product_id").isin([2, 4, 35]))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 390, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("min_quantity") > 2.8)
    .select("unit_price_bucket", "min_quantity", "avg_unit_price")
    .orderBy("unit_price_bucket")
    .limit(50)
)
