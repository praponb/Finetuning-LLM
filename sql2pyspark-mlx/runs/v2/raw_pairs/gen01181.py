from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.col("store_id").isin([1, 6])).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_semi")
    .select(F.col("stores.id"), F.col("stores.city"))
)
