from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("products.launch_date"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
    )
    .orderBy("launch_date")
)
