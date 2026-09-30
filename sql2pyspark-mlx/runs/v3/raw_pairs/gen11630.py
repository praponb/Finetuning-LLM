from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Berlin"]))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("east_sqft") > 77)
    .select("city", "east_sqft", "avg_sqft")
    .orderBy(F.col("east_sqft").desc())
)
