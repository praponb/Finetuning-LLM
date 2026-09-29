from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("a.balance") >= 25000)
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.col("a.status"),
    )
)
