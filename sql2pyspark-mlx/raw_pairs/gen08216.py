from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull() & (~F.col("category").isin(["Fashion", "Sports", "Books"]) | (F.size("tags") >= 2)))
    .agg(F.count("*").alias("num_products"))
    .orderBy("num_products")
)
