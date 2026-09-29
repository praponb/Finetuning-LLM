from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.year("opened_date") == 2023) | F.col("city").isin(["Bangkok", "Singapore", "New York"]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") < 5)
)
