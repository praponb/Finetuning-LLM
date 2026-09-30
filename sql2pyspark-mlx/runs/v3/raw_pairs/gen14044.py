from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["Central", "East", "South", "North"])
    .agg(F.avg("sqft"))
    .orderBy("city")
)
