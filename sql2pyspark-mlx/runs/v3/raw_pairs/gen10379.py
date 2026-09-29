from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("unit_price_count") < 1)
)
