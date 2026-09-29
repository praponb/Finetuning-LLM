from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("total_sqft") >= 29000)
)
