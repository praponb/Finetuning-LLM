from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "Tokyo"]))
    .groupBy("city")
    .pivot("region", ["Central", "South", "West"])
    .agg(F.avg("sqft"))
)
