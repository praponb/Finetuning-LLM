from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-07-10").cast("date"))
    .select("*", F.substring("status", 1, 2).alias("status_prefix"))
)

enriched = (
    valid
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "inner")
    .filter(F.col("o.status") == "PENDING")
    .select("a.*", F.col("o.discount"))
)

valid_payments = (
    enriched
    .filter(F.col("paid_at") >= F.lit("2023-10-18").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "status_prefix",
        "discount",
        F.to_date("paid_at").alias("paid_at_date"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

remaining_keys = (
    valid_payments
    .select("method", "order_id")
    .subtract(
        df_payments
        .filter(F.col("amount").isNotNull())
        .select("method", "order_id")
    )
)

kept_rows = (
    valid_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "paid_at", "status", "status_prefix", "discount", "method_filled")
    .orderBy(F.col("paid_at").desc())
)
