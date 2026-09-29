from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 11)
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 200, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("avg_quantity") < 70)
    .orderBy(F.col("avg_quantity").desc())
)
