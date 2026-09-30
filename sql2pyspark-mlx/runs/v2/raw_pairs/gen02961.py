from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.count("price").alias("price_count"))
    .filter(F.col("price_count") >= 9)
    .select("brand", "price_count")
)
