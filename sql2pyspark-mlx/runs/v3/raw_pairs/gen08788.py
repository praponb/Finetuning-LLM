from pyspark.sql import functions as F

filtered_stores = df_stores.filter(~F.col("store_name").like("%Riversi%"))

sqft_bucket_summary = (
    filtered_stores
    .groupBy(
        F.when(F.col("sqft") >= 13489, "high").when(F.col("sqft") >= 6730, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max("sqft").alias("max_sqft"))
)

result = (
    sqft_bucket_summary
    .filter(F.col("max_sqft") < 12000)
    .orderBy("sqft_bucket")
)
