from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2025)
    .select("id", "product_name", F.explode("tags").alias("tag_name"))
)
