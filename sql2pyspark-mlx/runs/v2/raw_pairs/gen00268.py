from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([8, 20, 35]) & F.col("quantity").between(4, 7)) | (F.col("quantity") < 6))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") >= 4)
    .drop("unique_orders")
    .orderBy(F.col("order_id").desc())
)
