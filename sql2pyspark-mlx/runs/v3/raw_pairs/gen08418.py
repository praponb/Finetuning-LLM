from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "right")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("stores.region"),
        F.col("products.product_name"),
    )
)
