from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 9, 11]))
    .select("id", "category", "product_name", F.explode("tags").alias("tag"))
)
