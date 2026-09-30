from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 200)
    .groupBy("plan", F.year("start_date").alias("start_year"))
    .agg(
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") >= 180)
    .select("plan", "start_year", "paused_count", "min_monthly_fee")
    .orderBy("plan")
)
