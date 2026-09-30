from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6]))
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .filter(F.col("ibs.stddev_quantity") > 55)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ibs.latest_last_restock"),
        F.col("ibs.stddev_quantity"),
    )
    .orderBy(F.col("latest_last_restock").desc())
)
