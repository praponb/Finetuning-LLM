from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .orderBy(F.col("unit_price_bucket").desc())
)
