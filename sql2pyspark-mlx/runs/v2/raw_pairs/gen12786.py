from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.month("last_restock").isin([2, 7, 11])).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_anti")
    .filter(F.col("stores.opened_date") < "2025-01-01")
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("stores.region"),
        F.col("stores.city"),
    )
)
