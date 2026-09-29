from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 450)
    .select("price", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)
