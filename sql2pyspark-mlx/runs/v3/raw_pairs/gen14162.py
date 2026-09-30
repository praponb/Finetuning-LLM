from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(~((F.col("order_id").isin([21, 28]) & (F.col("quantity") >= 5)) | (F.col("unit_price") <= 140)))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

order_items_stats = (
    valid_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("line_total") >= 68, "high").when(F.col("line_total") >= 23, "mid").otherwise("low").alias("line_total_bucket"),
    )
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    order_items_stats
    .filter(F.col("revenue") < 39)
    .select("unit_price_bucket", "revenue")
    .orderBy("revenue")
)
