from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("end_date") >= "2025-08-01")
    .withColumn("end_date_plus_29d", F.date_add("end_date", 29))
)

summary = (
    filtered_subscriptions
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
        F.count("*").alias("cnt"),
    )
)

result = summary.select("status", "pct_paused", "cnt")
