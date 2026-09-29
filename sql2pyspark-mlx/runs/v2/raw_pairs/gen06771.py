from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .groupBy(F.col("products.supplier_id"))
    .agg(
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
    )
    .filter(F.col("max_quantity") >= 220)
    .orderBy("supplier_id")
)
