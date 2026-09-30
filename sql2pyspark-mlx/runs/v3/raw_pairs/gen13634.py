from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 5, 6]))
    .select("id", "category", F.explode("tags").alias("label"))
)
