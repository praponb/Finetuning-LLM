from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([10, 11, 12]) | (F.col("city") == "Tokyo"))
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
)
