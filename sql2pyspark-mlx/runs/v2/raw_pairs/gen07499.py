from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.year("end_date").alias("end_year"))
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") > 41)
)
