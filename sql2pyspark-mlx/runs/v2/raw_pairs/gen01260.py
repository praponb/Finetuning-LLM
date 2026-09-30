from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.min("start_date").alias("min_start_date"))
    .orderBy("status")
)
