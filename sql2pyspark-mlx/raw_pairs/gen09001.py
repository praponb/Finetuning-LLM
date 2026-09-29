from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .filter(F.col("inventory.product_id").isin([1, 10]))
    .select(
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.col("inventory.last_restock"),
    )
    .orderBy(F.col("store_name").desc(), "opened_date")
)
