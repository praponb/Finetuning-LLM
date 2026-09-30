from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"))
    .filter(F.col("fashion_count") >= 3)
    .select("brand", "fashion_count")
    .orderBy("brand", F.col("fashion_count").desc())
)
