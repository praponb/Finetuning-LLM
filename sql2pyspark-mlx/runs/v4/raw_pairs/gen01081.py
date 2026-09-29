from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 12349)
    .groupBy("city")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 6200)
    .select("city", "avg_sqft", "min_sqft")
)
