from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["status", "txn_type"], ["status"]],
        F.col("d.status").alias("status"),
        F.col("e.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.amount")).alias("avg_amount"),
        F.max(F.col("e.amount_int")).alias("max_amount_int"),
    )
)
