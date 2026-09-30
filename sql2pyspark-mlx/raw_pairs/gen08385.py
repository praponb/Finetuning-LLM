from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("region") == "South").alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_semi")
    .select(F.col("inventory.id"), F.col("inventory.last_restock"))
    .orderBy(F.col("last_restock").desc(), "id")
    .limit(10)
)
