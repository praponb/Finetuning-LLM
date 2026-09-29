from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("start_date") < "2024-09-14")

summary = (
    valid_subscriptions
    .groupBy("status", F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
)

result = (
    summary
    .filter(F.col("standard_count") >= 75)
    .orderBy("end_month", "status")
    .limit(3)
)
