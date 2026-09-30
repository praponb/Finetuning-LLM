from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Globex"]))
    .select("id", "category", F.explode("tags").alias("label"))
)
