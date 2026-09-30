from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"), "plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") >= 65)
    .orderBy("plan")
)
