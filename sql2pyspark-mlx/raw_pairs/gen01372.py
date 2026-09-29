from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.account_type") == "CREDIT") & (F.to_date(F.col("t.txn_time")) < "2025-07-01"))
    .select(
        F.col("t.is_flagged"),
        F.col("t.merchant"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("c.city"),
    )
)
