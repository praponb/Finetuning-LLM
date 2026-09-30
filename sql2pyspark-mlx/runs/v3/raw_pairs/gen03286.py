from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([8, 10, 12, 28]))
    .withColumn("adjusted_monthly_fee", F.round((F.col("monthly_fee") * 1.07), 2))
)

status_summary = (
    valid_subscriptions
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    status_summary
    .filter(F.col("cancelled_count") > 58)
    .select("status", "cancelled_count")
    .orderBy("cancelled_count")
)
