from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "outer")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("products.category") == "Home")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("products.supplier_id"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.datediff(F.current_date(), F.col("inventory.last_restock")).alias("days_since_last_restock"),
    )
)
