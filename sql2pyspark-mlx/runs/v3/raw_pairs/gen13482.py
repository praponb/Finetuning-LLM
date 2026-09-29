from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter((F.col("t.amount") >= 2600) & F.month(F.col("a.opened_date")).isin([1, 5, 11]))
    .select(
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
    )
    .orderBy(F.col("txn_time").desc(), F.col("balance").desc())
)
