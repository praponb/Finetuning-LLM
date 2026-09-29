from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.lower("store_name").like("%own%"))

agg = (
    valid_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 15687, "high").when(F.col("sqft") >= 6141, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.avg("sqft").alias("avg_sqft"))
)

result = (
    agg
    .filter(F.col("avg_sqft") < 5700)
    .select("city", "avg_sqft")
    .orderBy("avg_sqft")
)
