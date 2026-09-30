from pyspark.sql import functions as F

base = df_subscriptions.filter(F.col("plan") == "basic")

subscriptions_stats = (
    base
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        F.when(F.col("monthly_fee") >= 225, "high").when(F.col("monthly_fee") >= 52, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
)

result = subscriptions_stats.select("start_month", "monthly_fee_bucket", "cnt")
