from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "West") & ~F.col("city").isin(["Tokyo", "Berlin", "New York"]) & (F.col("sqft") < 10082))
    .groupBy("city", "region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
    )
    .filter(F.col("avg_sqft") < 16000)
    .orderBy("avg_sqft", F.col("chiang_mai_sqft").desc())
)
