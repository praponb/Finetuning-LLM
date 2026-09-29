from pyspark.sql import functions as F

src = df_order_items.filter((F.col("unit_price") >= 320) & F.col("quantity").isNotNull())

unit_price_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("order_item_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    unit_price_bucket_summary
    .select("unit_price_bucket", "min_unit_price")
    .orderBy(F.col("unit_price_bucket").desc())
)
