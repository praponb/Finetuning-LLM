from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(~F.col("region").isin(["Central", "North"]) & (F.col("sqft") < 9013))
)

summary = (
    recent_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
    )
)

result = (
    summary
    .filter(F.col("tokyo_count") >= 10)
    .select("opened_month", "west_count")
    .orderBy(F.col("west_count").desc(), "opened_month")
    .limit(10)
)
