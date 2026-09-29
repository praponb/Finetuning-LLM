from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports", "Electronics"]))
    .select("price", F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)
