from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.account_id").isin([3, 10, 34]))
    .select(
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("t.account_id"),
        F.col("t.amount"),
    )
)
