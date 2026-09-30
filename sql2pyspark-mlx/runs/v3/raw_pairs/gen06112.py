from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.date_trunc("YEAR", F.col("t.txn_time")).alias("txn_time_period"),
    )
)
