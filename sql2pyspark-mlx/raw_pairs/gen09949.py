from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "CANCELLED"]) & ((F.col("customer_id") == 16) | F.col("plan").isin(["basic", "premium", "enterprise"])))
)

summary = (
    filtered_subscriptions
    .groupBy("plan", "status")
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
)

result = summary.filter(F.col("unique_statuss") >= 10)
