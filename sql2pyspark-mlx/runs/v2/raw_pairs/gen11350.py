from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["South", "North"])
    .agg(F.count("sqft"))
)
