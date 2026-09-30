from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["West", "Central", "South"])
    .agg(F.avg("sqft"))
)
