from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 13997, "high").when(F.col("sqft") >= 2873, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max("sqft").alias("max_sqft"))
    .filter(F.col("max_sqft") < 15900)
    .orderBy("sqft_bucket", "city")
    .limit(20)
)
