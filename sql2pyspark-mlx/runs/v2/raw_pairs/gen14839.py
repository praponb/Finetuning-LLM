from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.year("start_date").alias("start_year"))
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") > 41)
    .select("plan", "start_year", "stddev_monthly_fee", "min_monthly_fee")
)
