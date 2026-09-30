from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
        F.col("products.product_name"),
        F.col("products.price"),
        F.col("suppliers.country"),
    )
)
