from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 12924)
    .groupBy("city")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
    .filter(F.col("pct_west") > 41)
)
