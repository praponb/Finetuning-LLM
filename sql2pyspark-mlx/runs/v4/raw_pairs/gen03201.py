from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12852, "high").when(F.col("sqft") >= 4069, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("pct_singapore") < 26)
    .orderBy(F.col("avg_sqft").desc(), F.col("pct_singapore").desc(), "sqft_bucket")
    .limit(20)
)
