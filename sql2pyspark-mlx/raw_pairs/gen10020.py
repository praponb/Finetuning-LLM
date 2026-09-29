from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("inventory.product_id").isin([1, 7, 10]))
    .select(
        F.col("inventory.quantity"),
        F.col("products.launch_date"),
        F.col("products.category"),
        F.col("suppliers.country"),
        F.col("suppliers.city"),
    )
    .orderBy("city")
)
