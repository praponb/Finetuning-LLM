from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .orderBy(F.col("revenue").desc())
)
