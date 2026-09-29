from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("product_id").isNull())

unit_price_bucket_summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    unit_price_bucket_summary
    .select("unit_price_bucket", "max_quantity", "unit_price_count")
    .orderBy("unit_price_bucket")
    .limit(10)
)
