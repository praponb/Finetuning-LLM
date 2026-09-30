from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc(), "plan")
)
