from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

remaining_keys = (
    df_orders
    .select("channel")
    .subtract(
        df_orders
        .filter(F.col("channel") == "web")
        .select("channel")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(CANCELLED|COMPLETED)$"), "GROUP_A").when(F.col("status").rlike("^(PENDING|SHIPPED)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
        F.col("max_subscription_monthly_fee"),
    )
)
