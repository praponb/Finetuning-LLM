from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .orderBy("min_last_restock", F.col("stddev_quantity").desc())
)
