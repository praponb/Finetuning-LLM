from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("tba.amount_range"),
    )
)
