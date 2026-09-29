from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2025-04-01")
    .select("id", "category", "product_name", F.explode("tags").alias("label"))
)
