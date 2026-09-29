from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("stores.store_name").like("Ol%") & (F.col("stores.region") == "North"))
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("products.cost"),
        F.col("products.category"),
    )
)
