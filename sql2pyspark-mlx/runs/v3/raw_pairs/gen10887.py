from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("unit_price") >= 180)

unit_price_bucket_summary = (
    valid_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = unit_price_bucket_summary
