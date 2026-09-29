from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Wa%"))
    .select("id", "price", F.explode("tags").alias("tag"))
)
