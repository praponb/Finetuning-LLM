from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([2, 7, 10]) & (F.col("a.balance") >= 22000))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("a.balance"),
    )
)
