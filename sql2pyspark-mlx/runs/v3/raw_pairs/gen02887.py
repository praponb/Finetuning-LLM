from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("opened_month", F.col("west_sqft").desc(), "region")
    .limit(10)
)
