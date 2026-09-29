from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") >= 170)
)
