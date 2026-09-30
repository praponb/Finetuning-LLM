from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 260)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .orderBy("unit_price_count")
)
