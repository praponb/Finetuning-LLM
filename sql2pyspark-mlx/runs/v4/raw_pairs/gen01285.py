from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "North"]))
    .groupBy("region")
    .pivot("city", ["Tokyo", "Singapore", "Chiang Mai"])
    .agg(F.count("sqft"))
)
