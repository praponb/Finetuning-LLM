from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.is_flagged"))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.concat_ws("-", F.col("t.merchant"), F.col("t.txn_type")).alias("merchant_txn_type_key"),
    )
    .orderBy("merchant_txn_type_key")
)
