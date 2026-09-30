from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .select("*", F.length("channel").alias("channel_length"))
)

result = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.order_date") >= F.col("sub.start_date")) & (F.col("s.order_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
