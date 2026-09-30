from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("last_restock").between("2023-07-01", "2024-02-01"))
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("I"), F.col("s.id") == F.col("I.store_id"), "inner")
    .select(F.col("s.sqft"), F.col("I.unique_products"))
    .orderBy(F.col("unique_products").desc())
)
