from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.array_contains("tags", "premium")).alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_anti")
    .filter(F.col("inventory.last_restock").between("2023-03-31", "2025-05-01"))
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
    )
    .orderBy("store_id")
)
