from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

remaining_keys = (
    df_orders
    .select("customer_id", "id")
    .exceptAll(
        df_orders
        .filter(F.col("amount").between(1040, 1150))
        .select("customer_id", "id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "status",
        "channel",
        F.when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "GROUP_B").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("min_subscription_monthly_fee").desc(), "customer_id")
)
