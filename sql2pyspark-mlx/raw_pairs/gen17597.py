from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

matched_keys = (
    valid
    .select("merchant", "account_id")
    .subtract(
        df_transactions
        .filter(~F.col("merchant").isin(["Shell", "7-Eleven"]))
        .select("merchant", "account_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select("a.*", F.col("p2.status"), F.col("p2.balance"))
)

base = (
    joined
    .filter((F.col("txn_time") > F.lit("2023-05-25").cast("date")) & F.col("status").isNotNull())
    .select("*", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)

joined_transactions = (
    base
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.balance").alias("account_balance"), F.col("p.account_type"))
)

cleaned = (
    joined_transactions
    .filter((~F.col("account_type").eqNullSafe(F.lit("CREDIT"))) & F.col("account_balance").between(48000, 63000))
    .withColumn("amount_filled_minus_amount_scaled", (F.col("amount_filled") - F.col("amount_scaled")))
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount_scaled"))).alias("fee_total_amount_scaled"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
)
