from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_time") < "2024-04-01").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(~F.col("a.account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)
