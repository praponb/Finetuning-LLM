from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["standard", "basic"]))
    .groupBy("plan", "customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.max("start_date").alias("latest_start_date"),
    )
)
