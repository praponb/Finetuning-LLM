from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["Central", "South", "North"])
    .agg(F.max("sqft"))
)
