from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.balance").between(43000, 65000) & F.col("t.account_id").isin([31, 32]))
    .select(
        F.col("t.is_flagged"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.account_type"),
    )
)
