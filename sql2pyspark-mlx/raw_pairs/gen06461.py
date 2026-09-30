from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Bangkok", "Chiang Mai"]) & F.col("region").isNotNull() & (F.col("sqft") != 9251))
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
    )
)
