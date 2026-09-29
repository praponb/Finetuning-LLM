from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .filter(F.col("inventory.last_restock") <= "2025-04-01")
    .select(
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
    )
)
