from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Tokyo", "Bangkok", "London"])
    .agg(F.avg("sqft"))
)
