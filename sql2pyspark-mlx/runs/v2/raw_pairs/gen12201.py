from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12797, "high").when(F.col("sqft") >= 5356, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .filter(F.col("pct_bangkok") < 36)
    .select("sqft_bucket", "pct_bangkok")
    .orderBy("sqft_bucket")
    .limit(5)
)
