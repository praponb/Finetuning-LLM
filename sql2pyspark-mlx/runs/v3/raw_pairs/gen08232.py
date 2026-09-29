from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("product_id").isin([11, 22]))
    .withColumn("unit_price_whole", F.floor("unit_price"))
)

summary = (
    valid_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    summary
    .filter(F.col("unit_price_range") > 28)
    .select("unit_price_bucket", "unit_price_range")
    .orderBy("unit_price_range")
)
