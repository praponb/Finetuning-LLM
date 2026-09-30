from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2024-09-08")
    .select("price", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)
