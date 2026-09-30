from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .select(
        F.col("products.stock_qty"),
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
    )
)
