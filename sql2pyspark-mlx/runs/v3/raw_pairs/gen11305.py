from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_semi")
    .filter(F.col("products.supplier_id") == 21)
    .select(F.col("products.id"), F.col("products.supplier_id"), F.col("products.cost"))
)
