from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 2, 4]) & F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("stddev_quantity") > 69)
    .orderBy("avg_quantity", F.col("product_id").desc())
)
