from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.customer_id").isin([6, 18]) & (F.year(F.col("a.opened_date")) == 2023))
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.col("t.amount"),
    )
)
