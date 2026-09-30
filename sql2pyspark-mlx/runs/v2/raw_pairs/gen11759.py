from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .groupBy(
        F.when(F.col("unit_price") >= 450, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 9)
    .orderBy("quantity_bucket")
)
