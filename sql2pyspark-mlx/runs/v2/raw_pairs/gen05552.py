from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .filter(~F.col("suppliers.supplier_name").like("%Trader%"))
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("products.stock_qty"),
        F.col("products.brand"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
    )
    .orderBy(F.col("brand").desc())
)
