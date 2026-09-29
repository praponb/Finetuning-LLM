from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 4, 7]))
    .select("id", "price", F.explode("tags").alias("tag_name"))
)
