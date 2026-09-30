from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_anti")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("stores.sqft"),
        F.col("stores.store_name"),
    )
    .orderBy(F.col("id").desc(), F.col("region").desc())
)
