from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .select(
        F.col("products.supplier_id"),
        F.col("products.product_name"),
        F.col("products.stock_qty"),
        F.col("inventory.product_id"),
    )
    .orderBy("product_id")
)
