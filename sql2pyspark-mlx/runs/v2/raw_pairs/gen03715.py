from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter(F.col("t.is_flagged") & F.col("a.customer_id").isin([2, 8, 21, 22]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
    )
    .orderBy("txn_type")
)
