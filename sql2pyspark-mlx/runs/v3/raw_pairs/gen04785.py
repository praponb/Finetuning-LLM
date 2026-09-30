from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "New York"]))
    .groupBy("city")
    .pivot("region", ["Central", "East"])
    .agg(F.sum("sqft"))
    .orderBy("city")
)
