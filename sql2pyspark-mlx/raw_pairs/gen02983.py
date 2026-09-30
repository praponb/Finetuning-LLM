from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("inventory.product_id").isin([2, 6, 8]) & (F.col("stores.sqft") != 11783))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("products.cost"),
        F.col("products.brand"),
        F.col("stores.store_name"),
        F.col("stores.region"),
    )
)
