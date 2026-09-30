from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.month(F.col("inventory.last_restock")).isin([3, 8, 9]))
    .groupBy(F.col("products.supplier_id"), F.col("products.category"))
    .agg(F.min(F.col("inventory.last_restock")).alias("first_last_restock"))
)
