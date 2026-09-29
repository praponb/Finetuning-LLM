from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["South", "West"])
    .agg(F.avg("sqft"))
)
