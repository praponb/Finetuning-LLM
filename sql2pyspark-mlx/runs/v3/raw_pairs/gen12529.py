from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 13)
    .select("id", "price", "product_name", F.explode("tags").alias("tag"))
)
