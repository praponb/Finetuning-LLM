from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"))
    .filter(F.col("acme_count") >= 75)
    .select("brand", "acme_count")
    .orderBy("brand", "acme_count")
    .limit(10)
)
