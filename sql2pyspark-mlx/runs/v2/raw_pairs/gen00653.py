from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .filter(F.col("inventory.store_id").isin([3, 6]) & F.col("products.stock_qty").isNotNull())
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("products.cost"),
        F.col("products.price"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
    )
)
