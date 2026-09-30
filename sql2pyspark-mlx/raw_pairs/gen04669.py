from pyspark.sql import functions as F

recent_stores = df_stores.filter((F.col("sqft") < 15736) & (F.col("sqft") != 15109))

summary = (
    recent_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = (
    summary
    .filter(F.col("sqft_count") > 9)
    .select("opened_month", "region", "sqft_count")
    .orderBy("sqft_count")
)
