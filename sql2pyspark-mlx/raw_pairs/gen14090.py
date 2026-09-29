from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .filter(F.col("products.product_name").like("%Pro%") & (F.col("products.cost") <= 620))
    .select(
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("products.category"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity").cast("string").alias("quantity_str"),
    )
)
