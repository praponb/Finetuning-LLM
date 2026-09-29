from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") < 90)
)
