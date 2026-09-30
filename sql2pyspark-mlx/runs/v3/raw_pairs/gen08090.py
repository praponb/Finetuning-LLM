from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2024)
    .groupBy("plan")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .orderBy("avg_monthly_fee")
)
