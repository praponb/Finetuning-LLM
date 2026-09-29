from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "clearance"))
    .select(
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
        F.col("products.supplier_id"),
        F.col("products.cost"),
        F.col("suppliers.city"),
        F.col("suppliers.country"),
    )
    .orderBy("city")
)
