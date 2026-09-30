from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["South", "Central"]) & (F.col("sqft") >= 5502))
    .groupBy("city", "region")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy(F.col("city").desc(), "region")
    .limit(5)
)
