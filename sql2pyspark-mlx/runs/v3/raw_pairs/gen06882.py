from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-06-19", "2024-07-01"))
    .select("id", "category", F.explode("tags").alias("tag_name"))
)
