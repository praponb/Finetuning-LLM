from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 28) & F.col("status").isin(["PAUSED", "CANCELLED", "EXPIRED"]))
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.min("start_date").alias("min_start_date"))
)
