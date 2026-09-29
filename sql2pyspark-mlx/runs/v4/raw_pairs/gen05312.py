from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("account_id", "txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_type").isin(["FEE", "CREDIT"]))
        .select("account_id", "txn_type")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.when(F.col("merchant").rlike("^(Grab|Starbucks)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(7-Eleven|Amazon)$"), "CORE").otherwise("EXTENDED").alias("merchant_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
