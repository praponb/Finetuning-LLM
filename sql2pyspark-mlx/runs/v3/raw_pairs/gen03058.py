from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2025-07-04", "2025-08-01"))
    .select("id", "product_name", "price", F.explode("tags").alias("label"))
)
