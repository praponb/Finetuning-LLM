from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Home", "Sports", "Fashion"]))
    .select("id", "category", "brand", F.explode("tags").alias("tag"))
)
