from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.cost") < 340)
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("products.product_name"),
        F.col("suppliers.city"),
    )
)
