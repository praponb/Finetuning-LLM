from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .groupBy("city")
    .pivot("region", ["North", "Central", "East"])
    .agg(F.max("sqft"))
)
