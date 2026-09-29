from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "New York"]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"),
    )
    .filter(F.col("avg_sqft") >= 5600)
)
