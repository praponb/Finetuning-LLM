from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "London", "Bangkok", "New York"])
    .agg(F.max("sqft"))
)
