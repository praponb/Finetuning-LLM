from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("inventory.product_id").isin([3, 10]) & F.col("inventory.product_id").isin([3, 5, 9, 10]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("stores.opened_date"),
    )
    .orderBy("store_id", "id")
    .limit(50)
)
