from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("a.account_type").isin(["CREDIT", "CHECKING"]) & (F.to_date(F.col("t.txn_time")) < "2025-05-01"))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("c.age"),
        F.trim(F.col("t.txn_type")).alias("clean_txn_type"),
    )
)
