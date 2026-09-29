from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select("id", "category", "product_name", F.explode("tags").alias("tag"))
)
