from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("opened_date") <= "2024-04-07")

opened_month_summary = (
    filtered_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("sqft") >= 12724, "high").when(F.col("sqft") >= 4930, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"))
)

result = (
    opened_month_summary
    .filter(F.col("south_count") < 45)
    .orderBy("opened_month", F.col("south_count").desc(), "sqft_bucket")
    .limit(10)
)
