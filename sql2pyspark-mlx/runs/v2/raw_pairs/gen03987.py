from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.year("start_date") >= 2023) & F.col("customer_id").isin([1, 8, 12, 15])))
    .groupBy("status", F.month("start_date").alias("start_month"))
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") < 150)
)
