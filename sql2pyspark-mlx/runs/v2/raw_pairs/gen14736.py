from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_time") >= "2023-09-01")
    .select(
        F.col("a.account_type"),
        F.col("t.amount"),
        F.month(F.col("a.opened_date")).alias("opened_date_month"),
    )
)
