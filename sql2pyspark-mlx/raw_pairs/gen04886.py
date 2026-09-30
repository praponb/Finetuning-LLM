from pyspark.sql import functions as F

rollup_totals = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("dim"), F.col("a.account_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.status").alias("status"), F.col("a.txn_type").alias("txn_type"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.amount")).alias("min_amount"),
    )
)

result = (
    rollup_totals
    .select(
        "txn_type",
        "min_amount",
        F.when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "OTHER_ZONE").when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "GROUP_B").otherwise("GROUP_A").alias("txn_type_zone"),
    )
    .orderBy("txn_type_zone", F.col("txn_type").desc())
)
