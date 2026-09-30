from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "CANCELLED", "ACTIVE"]) & (F.col("plan") == "standard"))
    .withColumn("end_date_minus_9d", F.date_sub("end_date", 9))
)

subscriptions_stats = (
    src
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
)

result = subscriptions_stats.orderBy("enterprise_monthly_fee")
