from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%iam%"))
    .groupBy("city")
    .pivot("region", ["West", "North", "East", "South"])
    .agg(F.max("sqft"))
    .orderBy("city")
)
