from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("products")
    .join(inventory_by_product.alias("inventory_by_product"), F.col("products.id") == F.col("inventory_by_product.product_id"), "inner")
    .filter(F.col("inventory_by_product.quantity_count") > 6)
    .select(
        F.col("products.launch_date"),
        F.col("products.price"),
        F.col("inventory_by_product.quantity_count"),
    )
)
