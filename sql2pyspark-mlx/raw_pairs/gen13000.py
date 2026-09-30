from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & (F.col("status") == "PAUSED"))
)

summary = (
    filtered_subscriptions
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        F.when(F.col("plan").isin(["basic", "standard"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    summary
    .filter(F.col("stddev_monthly_fee") >= 63)
    .select("start_month", "stddev_monthly_fee")
    .orderBy(F.col("stddev_monthly_fee").desc())
)
