from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull())
    .groupBy("city")
    .pivot("region", ["Central", "East", "West", "South"])
    .agg(F.sum("sqft"))
    .orderBy("city")
)
