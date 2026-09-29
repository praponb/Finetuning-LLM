from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "London")
    .groupBy("region", "city")
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.max("opened_date").alias("latest_opened_date"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("central_count") >= 2)
)
