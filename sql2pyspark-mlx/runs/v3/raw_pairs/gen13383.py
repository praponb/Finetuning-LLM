from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "smart watch")
    .select("price", F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.col("avg_price").desc(), "label")
)
