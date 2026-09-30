from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .orderBy("west_sqft")
)
