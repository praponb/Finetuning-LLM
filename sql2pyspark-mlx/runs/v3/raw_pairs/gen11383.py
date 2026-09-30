from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("region") != "East") & F.col("store_name").like("%Cent%"))

opened_quarter_summary = (
    filtered_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
)

result = opened_quarter_summary.filter(F.col("pct_central") >= 20)
