from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("monthly_fee") > 130)

summary = (
    recent_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
)

result = summary.orderBy("start_month")
