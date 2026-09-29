from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-06-13").cast("date"))
    .withColumn("method_lower", F.lower("method"))
)

kept_keys = (
    cleaned
    .select("status")
    .exceptAll(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("status")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.shipping_fee"))
)

enriched_payments = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.amount").alias("order_amount"))
)

kept_keys_2 = (
    enriched_payments
    .select("status")
    .intersectAll(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
        .select("status")
    )
)

kept_rows = (
    enriched_payments
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["channel", "payment_status"], ["channel"]],
        F.col("o.channel").alias("channel"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.order_amount")).alias("max_order_amount"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("max_order_amount").desc())
)
