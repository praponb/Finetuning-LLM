from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("stddev_monthly_fee") >= 39)
    .orderBy("status_group")
)
