from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") > 960)
)
