from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.merchant").isin(["Shell", "Lazada", "7-Eleven"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("a.balance"),
        F.col("a.opened_date"),
    )
    .orderBy("balance", "txn_time", "id")
    .limit(10)
)
