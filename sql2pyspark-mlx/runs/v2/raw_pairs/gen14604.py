from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 10138)
    .groupBy("city")
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .filter(F.col("avg_sqft") > 60)
    .orderBy("city")
    .limit(5)
)
