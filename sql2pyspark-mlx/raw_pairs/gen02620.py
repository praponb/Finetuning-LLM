from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "right")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.last_restock"),
        F.col("products.supplier_id"),
        F.col("suppliers.country"),
    )
)
