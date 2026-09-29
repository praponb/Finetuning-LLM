from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.region").isin(["South", "West", "Central"]))
    .select(
        F.col("stores.city"),
        F.col("inventory.last_restock"),
        F.substring(F.col("stores.region"), 1, 3).alias("region_prefix"),
    )
)
