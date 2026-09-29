from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
)
