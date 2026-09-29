from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "London"]))
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
    .filter(F.col("east_sqft") < 46)
    .drop("east_sqft")
    .orderBy(F.col("max_sqft").desc(), "region")
    .limit(1)
)
