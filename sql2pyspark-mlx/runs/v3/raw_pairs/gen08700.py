from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .select(
        F.col("inventory.quantity"),
        F.col("stores.opened_date"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
        F.round(F.col("inventory.quantity"), 1).alias("quantity_rounded"),
    )
)
