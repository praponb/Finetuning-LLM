from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
    )
    .orderBy("central_count")
)
