from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("ibs.avg_quantity"),
        F.col("ibs.inventory_count"),
    )
)
