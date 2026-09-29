from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 5))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)
