from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2023-04-01")
    .select("id", "brand", "category", F.explode("tags").alias("label"))
)
