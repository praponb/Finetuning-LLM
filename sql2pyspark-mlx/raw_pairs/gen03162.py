from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum("sqft").alias("total_sqft"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
)
