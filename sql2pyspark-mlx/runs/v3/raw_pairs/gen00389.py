from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("customer_id").isin([27, 37, 38]))
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

subscriptions_stats = (
    src
    .groupBy("status", F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("days_since_end_date")).otherwise(0)).alias("standard_days_since_end_date"),
        F.max("start_date").alias("last_start_date"),
    )
)

result = subscriptions_stats.orderBy(F.col("standard_days_since_end_date").desc())
