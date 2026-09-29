from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("city") != "Chiang Mai").alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_anti")
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("inventory.last_restock"),
    )
    .orderBy("product_id", F.col("id").desc())
)
