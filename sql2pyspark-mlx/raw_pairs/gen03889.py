from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
