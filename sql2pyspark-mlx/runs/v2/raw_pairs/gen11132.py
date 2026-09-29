from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .groupBy("city")
    .pivot("region", ["West", "South", "East"])
    .agg(F.max("sqft"))
)
