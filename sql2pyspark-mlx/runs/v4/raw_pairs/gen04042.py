from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.year(F.col("a.opened_date")) == 2023)
    .select(
        F.col("t.merchant"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("a.status"),
        F.trim(F.col("a.account_type")).alias("clean_account_type"),
    )
)
