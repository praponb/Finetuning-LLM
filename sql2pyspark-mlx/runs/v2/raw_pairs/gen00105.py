from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 9, 12]))
    .groupBy("status")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)
