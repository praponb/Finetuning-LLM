from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["North", "East"])
    .agg(F.avg("sqft"))
)
