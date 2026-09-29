from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("merchant", "id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube("account_id", F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.max(F.when(F.col("txn_type") == "FEE", F.col("amount"))).alias("fee_max_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy("account_id")
)
