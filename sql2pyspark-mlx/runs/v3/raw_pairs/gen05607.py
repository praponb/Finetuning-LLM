from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("t.txn_type").isin(["REFUND", "CREDIT", "FEE"]))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.email"),
    )
    .orderBy(F.col("balance").desc())
)
