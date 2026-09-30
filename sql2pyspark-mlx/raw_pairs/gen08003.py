from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("products.brand"),
        F.col("stores.sqft"),
        F.round(F.col("inventory.quantity"), 1).alias("quantity_rounded"),
    )
)
