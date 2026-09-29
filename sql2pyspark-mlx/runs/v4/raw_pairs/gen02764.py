from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.month("start_date").isin([4, 8, 12]))

agg = (
    filtered_subscriptions
    .groupBy(
        F.quarter("start_date").alias("start_quarter"),
        F.when(F.col("plan").isin(["enterprise", "standard"]), "core").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = (
    agg
    .filter(F.col("avg_monthly_fee") >= 21)
    .orderBy("avg_monthly_fee")
)
