from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("region").isin(["South", "East", "Central"]) & F.col("city").isin(["Chiang Mai", "Berlin", "Bangkok"]))
)

agg = (
    filtered_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("sqft") >= 13722, "high").when(F.col("sqft") >= 7991, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"))
)

result = (
    agg
    .filter(F.col("south_count") < 78)
    .select("opened_month", "sqft_bucket", "south_count")
    .orderBy("opened_month", "sqft_bucket")
)
