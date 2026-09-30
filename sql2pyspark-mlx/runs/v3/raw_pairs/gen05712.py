from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("product_id").isin([5, 15, 37]) & F.col("quantity").between(3, 6)) | (F.col("order_id") == 8))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
)

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("unit_price_whole") >= 71, "high").when(F.col("unit_price_whole") >= 17, "mid").otherwise("low").alias("unit_price_whole_bucket"),
    )
    .agg(
        (F.max("unit_price_whole") - F.min("unit_price_whole")).alias("unit_price_whole_range"),
    )
)

result = (
    order_items_stats
    .select("unit_price_bucket", "unit_price_whole_bucket", "unit_price_whole_range")
    .orderBy("unit_price_bucket")
)
