from pyspark.sql import functions as F

base = df_order_items.filter(F.col("product_id").isNull() & (F.col("quantity") >= 6))

unit_price_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    unit_price_bucket_summary
    .filter(F.col("unit_price_range") > 72)
    .select("unit_price_bucket", "unit_price_range")
    .orderBy(F.col("unit_price_range").desc())
)
