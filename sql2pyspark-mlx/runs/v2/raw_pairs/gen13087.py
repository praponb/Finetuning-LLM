from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
)
