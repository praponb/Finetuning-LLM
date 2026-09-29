from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") > 37)
    .orderBy(F.col("stddev_quantity").desc())
)
