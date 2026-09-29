from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter((F.col("t.txn_time") >= "2025-08-26") & (F.col("t.txn_time") < "2025-09-01"))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.date_trunc("WEEK", F.col("a.opened_date")).alias("opened_date_period"),
    )
    .orderBy(F.col("amount").desc())
)
