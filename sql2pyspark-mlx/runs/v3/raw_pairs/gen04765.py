from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("quantity") != 205)
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("ibs"), F.col("s.id") == F.col("ibs.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("ibs.unique_products"))
    .orderBy("city")
)
