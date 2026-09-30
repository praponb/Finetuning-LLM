from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2024)
    .select("id", "category", F.explode("tags").alias("label"))
)
