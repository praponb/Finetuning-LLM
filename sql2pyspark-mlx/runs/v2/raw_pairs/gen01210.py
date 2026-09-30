from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Fashion", "Home"]))
    .select("id", "product_name", F.explode("tags").alias("label"))
)
