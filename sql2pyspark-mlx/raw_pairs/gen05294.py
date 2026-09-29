from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() | F.month("end_date").isin([2, 8, 9]))
    .groupBy(F.quarter("end_date").alias("end_quarter"), "plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)
