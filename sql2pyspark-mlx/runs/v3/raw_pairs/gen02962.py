from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") >= 4800)
    .orderBy("max_sqft", "avg_sqft")
)
