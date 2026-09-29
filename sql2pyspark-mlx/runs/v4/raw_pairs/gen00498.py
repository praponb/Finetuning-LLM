from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 10430)
    .groupBy("city")
    .pivot("region", ["North", "Central", "West", "South"])
    .agg(F.avg("sqft"))
)
