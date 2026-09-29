from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["West", "North"])
    .agg(F.max("sqft"))
)
