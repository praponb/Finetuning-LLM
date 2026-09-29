from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 33)
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"), "plan")
    .agg(F.max("start_date").alias("last_start_date"))
)
