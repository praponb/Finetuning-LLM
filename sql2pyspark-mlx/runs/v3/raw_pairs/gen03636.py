from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Sports", "Electronics", "Grocery"]))
    .select("id", "category", "brand", F.explode("tags").alias("tag"))
)
