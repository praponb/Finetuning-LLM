from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
)
