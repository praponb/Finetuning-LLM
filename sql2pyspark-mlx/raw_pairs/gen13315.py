from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(230, 300) | F.col("order_id").isin([5, 8, 17]))
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 50, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") >= 4.7)
)
