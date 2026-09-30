from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(~F.col("account_type").isin(["CHECKING", "LOAN"])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_anti")
    .filter(F.col("transactions.is_flagged"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.txn_type"),
        F.col("transactions.txn_time"),
    )
    .orderBy(F.col("txn_type").desc())
)
