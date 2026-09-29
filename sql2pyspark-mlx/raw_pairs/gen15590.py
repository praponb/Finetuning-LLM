from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

base = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-07-03")))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("DIRECT")))
)

kept = (
    base
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date").between("2023-06-01", "2025-02-01")), "left_semi")
)

matched = (
    kept
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.order_date") >= F.col("sub.start_date")) & (F.col("e.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept_keys = (
    matched
    .select("id", "status")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") < 39)
        .select("id", "status")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("status")
    .intersectAll(
        df_orders
        .filter(F.col("order_date") > "2025-02-01")
        .select("status")
    )
)

reconciled_orders = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_semi")
)

remaining_keys_2 = (
    not_flagged
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("customer_id") == 12)
        .select("id")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "channel_filled",
        "subscription_id",
        F.col("max_order_shipping_fee"),
    )
    .orderBy("id")
)
