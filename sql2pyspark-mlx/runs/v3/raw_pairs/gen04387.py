from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "CANCELLED"]))
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .pivot("plan", ["basic", "enterprise"])
    .agg(F.avg("monthly_fee"))
    .orderBy("start_month")
)
