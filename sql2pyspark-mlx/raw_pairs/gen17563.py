from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 450, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("*").alias("total_count"),
    )
)
