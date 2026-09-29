from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("suppliers.city").isin(["Bangkok", "Tokyo"]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("products.price"),
        F.col("products.category"),
        F.col("suppliers.rating"),
        F.col("suppliers.country"),
    )
)
