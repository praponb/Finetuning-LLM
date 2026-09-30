from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([11, 37, 38]))
    .withColumn("start_date_month_end", F.last_day("start_date"))
    .withColumn("status_lower", F.lower("status"))
)

result = (
    cleaned
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "CANCELLED", "PAUSED"])
    .agg(F.avg("monthly_fee").alias("amount"))
    .select(
        "plan",
        F.col("ACTIVE").alias("active"),
        F.col("CANCELLED").alias("cancelled"),
        F.col("PAUSED").alias("paused"),
    )
)
