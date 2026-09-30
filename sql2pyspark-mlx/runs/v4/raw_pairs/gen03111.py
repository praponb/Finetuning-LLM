from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 13449)
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .filter(F.col("west_sqft") < 24)
    .orderBy(F.col("west_sqft").desc())
)
