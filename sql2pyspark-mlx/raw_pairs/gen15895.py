from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-07-01")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .filter(F.col("west_sqft") >= 31)
)
