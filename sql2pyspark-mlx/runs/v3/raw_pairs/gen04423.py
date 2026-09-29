from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 9)
    .orderBy(F.col("unit_price_bucket").desc())
)
