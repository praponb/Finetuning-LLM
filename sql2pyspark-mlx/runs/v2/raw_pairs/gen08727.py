from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise", "standard"]) & (F.col("monthly_fee") < 180) & F.col("customer_id").isNull())
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

summary = (
    valid_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = summary.filter(F.col("pct_cancelled") > 74)
