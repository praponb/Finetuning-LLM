from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("region") == "East")
    .withColumn("adjusted_sqft", F.round((F.col("sqft") * 1.1), 2))
)

stores_stats = (
    recent_stores
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count("*").alias("total_count"),
    )
)

result = (
    stores_stats
    .filter(F.col("total_count") > 9)
    .orderBy(F.col("singapore_count").desc(), "city")
    .limit(20)
)
