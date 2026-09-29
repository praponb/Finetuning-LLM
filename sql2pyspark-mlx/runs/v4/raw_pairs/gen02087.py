from pyspark.sql import functions as F

with_window = (
    df_sessions
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "subscription_id",
        "monthly_fee",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "GROUP_A").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy("device", F.col("country").desc())
)
