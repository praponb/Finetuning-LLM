from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"))
)
