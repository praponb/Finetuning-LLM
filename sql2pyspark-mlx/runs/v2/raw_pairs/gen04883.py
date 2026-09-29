from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 70)
    .groupBy("status")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)
