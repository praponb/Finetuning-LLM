from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "left")
    .filter(F.col("ibs.quantity_count") >= 8)
    .select(F.col("s.city"), F.col("ibs.unique_products"), F.col("ibs.quantity_count"))
)
