from pyspark.sql import functions as F

result = (
    df_stores
    .select("store_name")
    .subtract(df_products.select(F.col("product_name").alias("store_name")))
    .orderBy("store_name")
)
