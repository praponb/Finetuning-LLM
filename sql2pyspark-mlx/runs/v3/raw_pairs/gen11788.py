from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 216)
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
