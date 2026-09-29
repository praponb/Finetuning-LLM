from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "outer")
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("stores.store_name"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
    )
    .orderBy(F.col("opened_date").desc())
)
