from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("sqft") <= 9498) & (F.col("sqft").between(12121, 15737) | (F.col("sqft") < 5054)))
)

stores_stats = (
    recent_stores
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = (
    stores_stats
    .filter(F.col("total_sqft") >= 51000)
    .select("region", "total_sqft")
)
