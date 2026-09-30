from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "right")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("stores.sqft"),
        F.col("stores.store_name"),
        F.col("products.price"),
        F.col("products.cost"),
        F.add_months(F.col("inventory.last_restock"), 3).alias("last_restock_plus_months"),
    )
)
