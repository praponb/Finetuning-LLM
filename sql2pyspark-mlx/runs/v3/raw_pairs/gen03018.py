from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 90) | (F.col("status") == "ACTIVE"))
    .groupBy("customer_id", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(F.min("end_date").alias("min_end_date"))
)
