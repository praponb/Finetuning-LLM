from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter((F.col("a.balance") <= 43000) & (F.col("a.balance") >= 42000))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("a.opened_date"),
        F.col("a.balance"),
    )
)
