from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") >= 41)
)
