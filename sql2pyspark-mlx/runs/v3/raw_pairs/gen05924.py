from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.product_id").isin([1, 3, 7, 10]))
    .groupBy(F.col("products.category"))
    .agg(
        F.max(F.col("inventory.last_restock")).alias("last_last_restock"),
        F.countDistinct(F.col("inventory.quantity")).alias("unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc())
)
