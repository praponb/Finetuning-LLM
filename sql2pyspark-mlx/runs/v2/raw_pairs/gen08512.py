from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 13710, "high").when(F.col("sqft") >= 8273, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
    .filter(F.col("pct_central") >= 42)
    .orderBy(F.col("sqft_bucket").desc())
)
