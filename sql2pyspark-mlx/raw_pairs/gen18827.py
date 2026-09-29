from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 3, 8]))
    .select("id", "product_name", F.explode("tags").alias("label"))
)
