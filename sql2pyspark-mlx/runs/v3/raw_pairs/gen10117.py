from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
        F.col("products.supplier_id"),
        F.col("suppliers.supplier_name"),
        F.round((F.col("products.cost") * 1.15), 2).alias("cost_adjusted"),
    )
)
