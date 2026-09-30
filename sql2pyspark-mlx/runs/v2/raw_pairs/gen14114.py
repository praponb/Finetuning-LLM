from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 8, 10]))
    .select("id", "price", "brand", F.explode("tags").alias("tag"))
)
