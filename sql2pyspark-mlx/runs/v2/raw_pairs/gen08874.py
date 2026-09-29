from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("store_name").like("%Riverside%")).alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_anti")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
