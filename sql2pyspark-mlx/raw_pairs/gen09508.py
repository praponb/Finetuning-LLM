from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "outer")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("products.cost") > 650)
    .select(
        F.col("inventory.last_restock"),
        F.col("products.brand"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
)
