from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "North", "West"])
    .agg(F.max("sqft"))
    .orderBy("city")
)
