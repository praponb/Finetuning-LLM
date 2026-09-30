from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("D%"))
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .orderBy(F.col("new_york_sqft").desc())
)
