from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 9, 11]))
    .select("id", "category", F.explode("tags").alias("label"))
)
