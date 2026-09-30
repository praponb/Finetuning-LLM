from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("region") == "West").alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_semi")
    .filter(F.col("inventory.store_id") == 5)
    .select(F.col("inventory.id"), F.col("inventory.store_id"))
)
