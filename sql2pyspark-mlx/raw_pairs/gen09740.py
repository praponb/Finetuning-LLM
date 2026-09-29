from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("sqft").between(6532, 10242))
    .withColumn("sqft_level", F.when(F.col("sqft") >= 16629, "High").when(F.col("sqft") >= 6196, "Medium").otherwise("Low"))
)

stores_stats = (
    recent_stores
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("avg_sqft") > 13200)
    .select("region", "min_sqft")
    .orderBy(F.col("min_sqft").desc(), "region")
    .limit(10)
)
