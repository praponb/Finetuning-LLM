from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "PAUSED")
    .groupBy("plan", "status")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 5)
    .drop("cnt")
    .orderBy("plan")
)
