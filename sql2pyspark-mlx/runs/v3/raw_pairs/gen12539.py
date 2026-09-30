from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["New York", "Chiang Mai", "Singapore", "Tokyo"])
    .agg(F.avg("sqft"))
    .orderBy("region")
)
