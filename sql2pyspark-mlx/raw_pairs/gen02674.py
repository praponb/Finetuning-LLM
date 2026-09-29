from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["South", "Central", "East", "North"])
    .agg(F.sum("sqft"))
    .orderBy("city")
)
