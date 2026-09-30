from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .groupBy("city")
    .pivot("region", ["Central", "West", "South", "North"])
    .agg(F.count("sqft"))
)
