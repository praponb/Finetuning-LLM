from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(~((F.col("sqft") != 7225) | F.col("sqft").between(8879, 10814)))
    .withColumn("sqft_rounded", F.round("sqft", 0))
)

agg = (
    recent_stores
    .groupBy("city")
    .agg(F.min("sqft_rounded").alias("min_sqft_rounded"))
)

result = (
    agg
    .filter(F.col("min_sqft_rounded") < 32)
    .select("city", "min_sqft_rounded")
    .orderBy("city")
    .limit(5)
)
