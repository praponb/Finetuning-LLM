from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["London", "Chiang Mai"])
    .agg(F.count("sqft"))
    .orderBy("region")
)
