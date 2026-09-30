from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["New York", "London", "Bangkok"])
    .agg(F.count("sqft"))
)
