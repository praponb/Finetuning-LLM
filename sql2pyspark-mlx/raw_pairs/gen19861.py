from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("products.supplier_id"),
        F.col("products.cost"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
    )
)
