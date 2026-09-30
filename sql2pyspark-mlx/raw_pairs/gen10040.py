from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.year(F.col("a.opened_date")) >= 2025)
    .select(
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("t.txn_time"),
    )
)
