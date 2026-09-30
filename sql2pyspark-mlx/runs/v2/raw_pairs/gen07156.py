from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.filter(F.year("last_restock") >= 2024).alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_anti")
    .select(F.col("products.id"), F.col("products.supplier_id"))
)
