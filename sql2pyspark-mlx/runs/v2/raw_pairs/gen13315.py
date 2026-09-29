from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Acme", "Globex", "Umbrella"]))
    .select("price", F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.col("avg_price").desc(), "tag")
)
