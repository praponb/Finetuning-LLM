from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2023-10-20")
    .select("id", "product_name", "brand", F.explode("tags").alias("tag_name"))
)
