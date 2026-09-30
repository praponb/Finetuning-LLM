from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .orderBy("pct_london", F.col("pct_west").desc())
)
