from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Bangkok", "London"]) & F.col("city").isin(["New York", "London"]))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .orderBy("opened_month")
)
