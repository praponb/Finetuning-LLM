from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & (F.col("opened_date") <= "2025-03-01") & F.col("city").isin(["Singapore", "New York", "Bangkok"]))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .filter(F.col("new_york_sqft") > 45)
    .orderBy("avg_sqft", "opened_month")
    .limit(20)
)
