from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-14 13:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

matched_keys = (
    valid
    .select("status")
    .exceptAll(
        df_payments
        .filter(F.col("method").isin(["promptpay", "card", "cash"]))
        .select("status")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.status").alias("order_status"),
        F.col("p.channel"),
        F.col("p.amount").alias("order_amount"),
    )
)

filtered = (
    joined
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-06-17")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_filled",
        "method_clean",
        "order_status",
        "channel",
        "order_amount",
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "method_clean",
        "order_status",
        "order_amount",
        "channel_status_key",
        F.initcap(F.trim("method")).alias("method_title"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.status").alias("order_status_2"), F.col("b.order_date"))
)

with_dims_payments = (
    with_dims
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.paid_at",
        "s.method_clean",
        "s.order_status",
        "s.method_title",
        F.col("p.amount").alias("order_amount_2"),
        F.col("p.status").alias("order_status_3"),
        F.col("p.channel"),
    )
)

result = (
    with_dims_payments
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("order_amount_2_tier", F.when(F.col("order_amount_2") >= 1280, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "order_amount_2_tier"], ["year_start"]],
        "year_start",
        "order_amount_2_tier",
    )
    .agg(
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.max("paid_at").alias("last_seen"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_rows"),
        F.percentile_approx("order_amount_2", 0.9).alias("order_amount_2_p90"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy("order_amount_2_p90")
)
