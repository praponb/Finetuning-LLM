from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
)
