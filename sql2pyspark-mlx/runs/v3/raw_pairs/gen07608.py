from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["standard", "enterprise"]) & F.col("status").isin(["CANCELLED", "PAUSED"]) & (F.col("start_date") < "2023-07-29"))
    .groupBy("plan", F.year("end_date").alias("end_year"))
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)
