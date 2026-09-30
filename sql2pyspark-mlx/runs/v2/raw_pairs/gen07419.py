from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("last_restock").alias("min_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy("min_last_restock")
)
