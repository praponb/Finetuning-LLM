from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 6612)
    .groupBy("city")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
    )
)
