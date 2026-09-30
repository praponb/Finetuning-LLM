from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Bangkok", "New York", "Tokyo", "Singapore"])
    .agg(F.avg("sqft"))
    .orderBy("region")
)
