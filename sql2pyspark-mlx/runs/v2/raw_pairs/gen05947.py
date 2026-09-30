from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.month("start_date").isin([3, 4, 8]) & F.col("monthly_fee").between(100, 120))
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

summary = (
    recent_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"), "plan")
    .agg(
        (F.max("days_since_end_date") - F.min("days_since_end_date")).alias("days_since_end_date_range"),
    )
)

result = (
    summary
    .filter(F.col("days_since_end_date_range") >= 63)
    .select("start_month", "days_since_end_date_range")
    .orderBy("start_month", "days_since_end_date_range")
)
