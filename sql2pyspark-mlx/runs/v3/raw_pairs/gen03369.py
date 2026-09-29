from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "West"])
    .agg(F.count("sqft"))
    .orderBy("city")
)
