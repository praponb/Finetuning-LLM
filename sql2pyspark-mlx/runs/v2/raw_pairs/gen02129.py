from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-02-01", "2024-06-01"))
    .select("id", "product_name", F.explode("tags").alias("label"))
)
