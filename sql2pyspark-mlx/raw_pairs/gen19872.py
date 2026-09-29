from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("cnt"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") >= 13800)
    .orderBy(F.col("max_sqft").desc(), "region")
    .limit(20)
)
