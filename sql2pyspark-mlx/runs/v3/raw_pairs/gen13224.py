from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2023)
    .groupBy("plan", F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.count("*").alias("subscription_count"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)
