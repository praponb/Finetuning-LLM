from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2025-05-01"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("merchant_length", F.length("merchant"))
)

matched_keys = (
    filtered
    .select("account_id")
    .intersectAll(
        df_transactions
        .filter(F.hour("txn_time").between(11, 17))
        .select("account_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") < "2023-04-01")
        .select("txn_type")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("merchant_length_tier", F.when(F.col("merchant_length") >= 63, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["merchant", "merchant_length_tier", "account_id"], ["merchant", "merchant_length_tier"], ["merchant"], []],
        "merchant",
        "merchant_length_tier",
        "account_id",
    )
    .agg(
        F.round(F.sum("is_flagged_flag") / F.nullif(F.count(F.when(F.col("merchant") == "Lazada", 1)), F.lit(0)), 2).alias("is_flagged_flag_per_lazada"),
        F.round(F.sum("is_flagged_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("is_flagged_flag_per_fee"),
    )
)
