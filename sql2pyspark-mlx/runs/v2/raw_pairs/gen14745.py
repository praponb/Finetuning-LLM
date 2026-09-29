from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("latest_last_restock"))
)

result = (
    df_products.alias("products")
    .join(inventory_by_product.alias("inventory_by_product"), F.col("products.id") == F.col("inventory_by_product.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.launch_date"),
        F.col("inventory_by_product.latest_last_restock"),
    )
    .orderBy("brand")
)
