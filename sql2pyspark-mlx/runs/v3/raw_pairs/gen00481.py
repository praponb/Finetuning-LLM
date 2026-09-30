from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["Central", "South", "West"])
    .agg(F.max("sqft"))
)
