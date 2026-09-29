from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Umbrella", "Stark"]))
    .select("id", "product_name", "brand", F.explode("tags").alias("tag_name"))
)
