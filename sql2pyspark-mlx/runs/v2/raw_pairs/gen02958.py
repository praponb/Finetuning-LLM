from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy("tokyo_count")
)
