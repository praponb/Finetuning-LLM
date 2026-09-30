from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-01-01", "2024-08-20"))
    .select("id", "category", "product_name", F.explode("tags").alias("tag_name"))
)
