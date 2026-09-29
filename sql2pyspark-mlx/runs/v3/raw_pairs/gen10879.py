from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("start_date") >= 2023) & F.col("customer_id").isin([10, 14, 18, 35]))
    .groupBy("status")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)
