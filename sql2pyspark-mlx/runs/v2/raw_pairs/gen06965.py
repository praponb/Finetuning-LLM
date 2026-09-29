from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-11-13")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("opened_quarter")
    .limit(3)
)
