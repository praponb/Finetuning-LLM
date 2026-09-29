from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "outer")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
        F.col("products.launch_date"),
        F.col("stores.opened_date"),
        F.col("stores.city"),
    )
)
