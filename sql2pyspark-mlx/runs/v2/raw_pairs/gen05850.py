from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("products.stock_qty").between(309, 342))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("products.supplier_id"),
        F.col("products.brand"),
        F.col("stores.opened_date"),
    )
)
