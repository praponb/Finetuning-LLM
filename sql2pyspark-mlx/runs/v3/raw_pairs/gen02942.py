from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-01-01")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
    )
    .orderBy(F.col("west_count").desc())
)
