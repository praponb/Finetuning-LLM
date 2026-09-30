from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_anti")
    .select(F.col("products.id"), F.col("products.stock_qty"), F.col("products.cost"))
)
