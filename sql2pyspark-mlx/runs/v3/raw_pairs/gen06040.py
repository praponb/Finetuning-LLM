from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("products.category") == "Books")
    .select(
        F.col("inventory.store_id"),
        F.col("stores.opened_date"),
        F.col("products.launch_date"),
    )
    .orderBy(F.col("store_id").desc())
)
