from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("sqft") == 14531) & (F.col("sqft") == 14360)) | (F.col("sqft") != 6854))
    .groupBy("city")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count("sqft").alias("sqft_count"),
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
    .orderBy("city", "sqft_range")
)
