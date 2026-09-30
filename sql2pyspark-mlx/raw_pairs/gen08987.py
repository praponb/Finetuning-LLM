from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("a.account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
    )
    .orderBy(F.col("status").desc())
)
