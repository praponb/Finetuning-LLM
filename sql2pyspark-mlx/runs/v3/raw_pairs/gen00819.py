from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-08-01", "2024-11-26"))
    .select("price", F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.col("avg_price").desc(), "tag")
)
