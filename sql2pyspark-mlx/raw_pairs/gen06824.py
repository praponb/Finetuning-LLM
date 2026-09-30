from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Grocery")
    .select("id", "cost", "price", "category", F.size("tags").alias("tags_count"))
)
