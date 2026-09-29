from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .pivot("plan", ["basic", "standard", "enterprise", "premium"])
    .agg(F.sum("monthly_fee"))
)
