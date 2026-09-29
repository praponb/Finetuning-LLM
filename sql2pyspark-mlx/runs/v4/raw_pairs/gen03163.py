from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["Central", "East"])
    .agg(F.sum("sqft").alias("amount"))
    .select("city", F.col("Central").alias("central"), F.col("East").alias("east"))
)
