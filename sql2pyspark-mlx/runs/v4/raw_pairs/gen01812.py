from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull())
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "New York", "Tokyo"])
    .agg(F.max("sqft"))
    .orderBy("region")
)
