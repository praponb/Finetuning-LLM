from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.store_name").like("%Downto%"))
    .groupBy(F.col("stores.region"))
    .agg(F.count(F.col("inventory.id")).alias("inventory_count"))
)
