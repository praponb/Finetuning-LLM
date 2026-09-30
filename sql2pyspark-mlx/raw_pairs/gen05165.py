from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter((F.col("products.supplier_id") == 7) & F.col("suppliers.country").isin(["SG", "US", "DE"]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("products.cost"),
        F.col("products.launch_date"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
    )
    .orderBy("quantity")
)
