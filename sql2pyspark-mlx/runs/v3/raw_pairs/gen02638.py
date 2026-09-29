from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") > 130)
    .orderBy("start_month")
)
