from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .withColumn("status_upper", F.upper("status"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
