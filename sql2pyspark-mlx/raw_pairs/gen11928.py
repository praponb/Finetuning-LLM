from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("singapore_count") >= 36)
    .orderBy("pct_chiang_mai")
)
