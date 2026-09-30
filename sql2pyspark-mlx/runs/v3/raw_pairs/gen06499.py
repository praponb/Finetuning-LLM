from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .filter(F.col("ibs.avg_quantity") > 210)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ibs.min_last_restock"),
        F.col("ibs.avg_quantity"),
    )
    .orderBy("id")
)
