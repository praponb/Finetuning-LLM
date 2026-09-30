from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("avg_quantity") > 4.9)
    .orderBy("total_count", "unit_price_bucket", "quantity_bucket")
    .limit(10)
)
