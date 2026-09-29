from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("west_count") >= 60)
    .select("opened_quarter", "west_count", "first_opened_date")
)
