from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & F.month("opened_date").isin([4, 5, 7]) & (F.col("sqft") >= 12397))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
        F.count("*").alias("store_count"),
    )
)
