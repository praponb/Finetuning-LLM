from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([3, 11]) & (F.col("unit_price").between(210, 270) | F.col("order_id").isin([27, 29, 38])))
)

unit_price_bucket_summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    unit_price_bucket_summary
    .select("unit_price_bucket", "min_quantity", "max_quantity")
    .orderBy(F.col("min_quantity").desc(), "unit_price_bucket")
    .limit(20)
)
