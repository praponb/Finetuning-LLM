from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(((F.year("start_date") == 2025) & (F.col("monthly_fee") > 130)) | (F.col("status") == "ACTIVE"))
)

summary = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"))
)

result = (
    summary
    .filter(F.col("enterprise_count") > 9)
    .select("status", "enterprise_count")
)
