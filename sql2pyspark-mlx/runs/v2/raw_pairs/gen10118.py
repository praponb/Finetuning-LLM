from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.month("end_date").alias("end_month"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") < 37)
    .select("plan", "end_month", "stddev_monthly_fee")
)
