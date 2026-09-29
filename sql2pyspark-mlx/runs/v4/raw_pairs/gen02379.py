from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.max("sqft").alias("max_sqft"))
    .filter(F.col("max_sqft") > 7000)
    .orderBy(F.col("max_sqft").desc(), "city")
    .limit(3)
)
