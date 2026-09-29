from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "New York", "Chiang Mai"]) & (F.col("sqft") < 7327))
    .withColumn("clean_city", F.trim("city"))
)
