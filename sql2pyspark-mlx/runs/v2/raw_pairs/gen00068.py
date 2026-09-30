from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Initech", "Globex"]))
    .select("id", "price", "brand", F.explode("tags").alias("tag_name"))
)
