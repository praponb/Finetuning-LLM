from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2024-09-01")
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
    .orderBy("plan")
)
