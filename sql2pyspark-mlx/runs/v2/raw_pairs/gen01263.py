from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Books")
    .select("id", "product_name", "brand", F.explode("tags").alias("label"))
)
