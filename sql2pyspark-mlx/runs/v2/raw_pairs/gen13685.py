from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Fashion", "Electronics", "Home"]))
    .select("id", "price", "brand", F.explode("tags").alias("label"))
)
