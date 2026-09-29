from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Stark", "Umbrella", "Acme"]))
    .select("id", "brand", "category", F.explode("tags").alias("tag_name"))
)
