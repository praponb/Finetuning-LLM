from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 32) & (F.col("quantity").between(4, 6) | (F.col("unit_price") <= 170)))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.min("quantity").alias("min_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 1)
    .drop("quantity_count")
)
