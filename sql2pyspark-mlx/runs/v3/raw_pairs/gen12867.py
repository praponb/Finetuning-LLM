from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 7)
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") < 76)
)
