from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["South", "West", "East"])
    .agg(F.count("sqft"))
    .orderBy("city")
)
