from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~(F.hour(F.col("t.txn_time")).between(10, 13) & (F.col("t.txn_time") >= "2023-09-01")))
    .select(
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("t.is_flagged"),
    )
    .orderBy("balance")
)
