from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") >= "2023-09-01") | (F.col("opened_date") < "2025-10-22"))
    .groupBy("region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") < 52)
)
