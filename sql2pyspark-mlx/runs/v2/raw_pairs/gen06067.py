from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 12]) & (F.col("region").isin(["East", "Central", "South"]) | (F.col("region") == "South")))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.avg("sqft").alias("avg_sqft"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .orderBy(F.col("total_sqft").desc(), F.col("avg_sqft").desc(), "opened_quarter")
    .limit(50)
)
