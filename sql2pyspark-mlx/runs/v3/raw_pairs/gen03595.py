from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.filter(F.col("cost").between(450, 620)).alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left_semi")
    .filter(F.col("store_sales.store_id") == 5)
    .select(F.col("store_sales.id"), F.col("store_sales.units"))
)
