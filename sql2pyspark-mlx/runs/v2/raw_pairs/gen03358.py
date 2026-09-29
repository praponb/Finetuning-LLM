from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-05-01")
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .orderBy(F.col("max_quantity").desc())
)
