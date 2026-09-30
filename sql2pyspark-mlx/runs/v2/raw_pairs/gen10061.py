from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .orderBy("end_month")
)
