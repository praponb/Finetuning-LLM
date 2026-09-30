from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("status", "order_id")
    .subtract(
        df_payments
        .filter(F.col("amount") <= 520)
        .select("status", "order_id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("amount") >= 1280, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("amount_p90") > 0)
)
