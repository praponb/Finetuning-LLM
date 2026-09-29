from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") > 2)
    .drop("unit_price_count")
)
