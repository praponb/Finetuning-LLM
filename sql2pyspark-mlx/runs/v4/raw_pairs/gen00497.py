from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "right")
    .filter(F.col("products.stock_qty") >= 274)
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("stores.opened_date"),
        F.col("products.stock_qty"),
    )
)
