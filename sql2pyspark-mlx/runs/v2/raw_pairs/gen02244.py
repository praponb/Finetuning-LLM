from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.year("start_date") >= 2023) & (F.col("status") != "ACTIVE"))
)

summary = (
    filtered_subscriptions
    .groupBy("status")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)

result = (
    summary
    .select("status", "monthly_fee_range")
    .orderBy("status", "monthly_fee_range")
)
