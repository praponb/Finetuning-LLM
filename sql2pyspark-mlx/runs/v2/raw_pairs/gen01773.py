from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2025-08-01")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("tokyo_count").desc())
)
