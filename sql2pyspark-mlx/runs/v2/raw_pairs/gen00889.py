from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("store_name").isNotNull() & (F.col("sqft") < 16063))

stores_stats = (
    recent_stores
    .groupBy(
        F.when(F.col("sqft") >= 14046, "high").when(F.col("sqft") >= 4380, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.min("sqft").alias("min_sqft"))
)

result = stores_stats.select("sqft_bucket", "min_sqft")
