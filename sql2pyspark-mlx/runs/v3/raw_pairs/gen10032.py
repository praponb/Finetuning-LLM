from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "outer")
    .filter((F.col("inventory.store_id") == 1) & (F.year(F.col("inventory.last_restock")) == 2024))
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("stores.sqft"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
    .orderBy(F.col("sqft").desc())
)
