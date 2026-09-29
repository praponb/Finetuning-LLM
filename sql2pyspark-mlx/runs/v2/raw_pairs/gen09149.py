from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("store_id") == 3)
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    df_products.alias("products")
    .join(inventory_by_product.alias("inventory_by_product"), F.col("products.id") == F.col("inventory_by_product.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("inventory_by_product.first_last_restock"),
    )
)
