from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 8))
    .groupBy("order_id", "product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc())
)
