from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("chiang_mai_count") < 31)
    .select("region", "city", "chiang_mai_count", "row_count")
)
