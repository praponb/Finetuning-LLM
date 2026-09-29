from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") > 35)
    .orderBy(F.col("stddev_monthly_fee").desc(), "plan")
    .limit(5)
)
