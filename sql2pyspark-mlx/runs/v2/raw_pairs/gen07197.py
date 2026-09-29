from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("unit_price_bucket").desc())
)
