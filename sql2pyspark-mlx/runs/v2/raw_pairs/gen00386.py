from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("customer_id").isin([27, 37, 38]))
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

summary = (
    src
    .groupBy(F.month("end_date").alias("end_month"), "plan")
    .agg(F.max("days_since_end_date").alias("max_days_since_end_date"))
)

result = (
    summary
    .select("end_month", "plan", "max_days_since_end_date")
    .orderBy(F.col("end_month").desc())
)
