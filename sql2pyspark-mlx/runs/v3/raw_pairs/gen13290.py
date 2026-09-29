from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter((F.col("a.account_type") != "LOAN") | F.hour(F.col("t.txn_time")).between(6, 12))
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("t.amount"),
        F.col("t.merchant"),
    )
    .orderBy(F.col("opened_date").desc())
)
