from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["CANCELLED", "EXPIRED", "PAUSED"]) | ~F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc(), F.col("unique_customers").desc())
)
