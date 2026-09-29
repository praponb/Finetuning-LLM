from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)
