from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("region") != "East") & F.col("sqft").between(4686, 9532) & ~F.col("city").isin(["Bangkok", "Chiang Mai"]))
    .withColumn("is_new_york", F.when(F.col("city") == "New York", 1).otherwise(0))
)

city_summary = (
    base
    .groupBy("city", "region")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
)

result = city_summary.select("city", "region", "new_york_sqft", "avg_sqft")
