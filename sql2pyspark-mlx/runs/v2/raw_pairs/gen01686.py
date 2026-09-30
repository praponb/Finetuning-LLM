from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.filter(F.upper(F.trim("product_name")) == "LAPTOP PRO 14").alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left_semi")
    .select(F.col("store_sales.id"), F.col("store_sales.store_id"))
    .orderBy(F.col("store_id").desc(), "id")
)
