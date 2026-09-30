from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"]))
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") >= 120)
    .drop("avg_monthly_fee")
    .orderBy(F.col("monthly_fee_count").desc(), "start_month")
    .limit(20)
)
