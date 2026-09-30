from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(F.to_date("txn_time") >= "2025-05-18")
        .select("merchant", "id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup(
        "merchant",
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount"))).alias("debit_total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount"))).alias("grab_total_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("grab_total_amount") > 0)
    .orderBy(F.col("merchant").desc_nulls_first())
)
