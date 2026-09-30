from pyspark.sql import functions as F

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-05-31").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
