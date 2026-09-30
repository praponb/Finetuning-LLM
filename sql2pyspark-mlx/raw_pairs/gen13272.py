from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 400) & (F.col("quantity") <= 6))
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.count("unit_price").alias("unit_price_count"))
    .orderBy(F.col("unit_price_bucket").desc())
)
