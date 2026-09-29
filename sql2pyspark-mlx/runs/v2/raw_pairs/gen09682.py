from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.product_id") == 9)
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("products.stock_qty"),
        F.col("products.product_name"),
        F.col("products.brand"),
    )
    .orderBy("product_name")
)
