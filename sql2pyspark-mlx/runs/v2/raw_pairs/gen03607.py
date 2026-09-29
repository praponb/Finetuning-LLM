from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(((F.col("end_date") > "2025-03-01") & F.col("customer_id").isNull()) | (F.col("monthly_fee") > 140))
    .withColumn("start_date_day", F.dayofmonth("start_date"))
)

subscriptions_stats = (
    base
    .groupBy("status", F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.min("start_date").alias("first_start_date"))
)

result = (
    subscriptions_stats
    .select("status", "start_month", "first_start_date")
    .orderBy("status", "first_start_date")
)
