from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("t.txn_type"),
        F.col("t.amount"),
    )
    .orderBy(F.col("balance").desc(), "txn_type")
)
