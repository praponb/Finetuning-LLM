from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("last_restock") >= "2023-04-16")

store_id_summary = (
    recent_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    store_id_summary
    .select("store_id", "min_quantity")
    .orderBy(F.col("min_quantity").desc(), F.col("store_id").desc())
    .limit(20)
)
