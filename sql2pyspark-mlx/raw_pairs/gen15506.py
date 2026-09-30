from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "right")
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("a.customer_id"),
        F.datediff(F.current_date(), F.col("a.opened_date")).alias("days_since_opened_date"),
    )
)
