from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & (F.col("region") == "East") & F.col("sqft").between(5325, 9939))
    .select("id", "region", F.trim("city").alias("clean_city"))
    .orderBy(F.col("id").desc(), "region")
)
