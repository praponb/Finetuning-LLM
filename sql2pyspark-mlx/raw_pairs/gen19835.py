from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.last_restock").between("2025-04-01", "2025-09-01") & F.col("inventory.store_id").isin([2, 3, 4, 6]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("products.price"),
    )
)
