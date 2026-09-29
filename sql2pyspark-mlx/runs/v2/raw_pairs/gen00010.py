from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("bangkok_sqft") < 72)
    .orderBy("region")
)
