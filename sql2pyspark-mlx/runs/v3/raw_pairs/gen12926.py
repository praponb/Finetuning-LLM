from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .filter(F.col("ibs.avg_quantity") >= 70)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ibs.avg_quantity"),
        F.col("ibs.latest_last_restock"),
    )
    .orderBy(F.col("avg_quantity").desc(), "region", "id")
    .limit(50)
)
