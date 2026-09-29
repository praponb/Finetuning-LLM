from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .select(F.col("inventory.id"), F.col("inventory.quantity"), F.col("stores.region"))
    .orderBy(F.col("id").desc())
)
