from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNull() | F.col("customer_id").isNotNull())
    .withColumn("status_upper", F.upper("status"))
)

subscriptions_stats = (
    valid_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("standard_count") >= 78)
    .orderBy(F.col("start_month").desc())
)
