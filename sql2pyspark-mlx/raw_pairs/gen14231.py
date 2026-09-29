from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .orderBy("unit_price_bucket")
    .limit(50)
)
