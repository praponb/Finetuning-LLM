from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("t.amount") > 1300)
    .select(
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.segment"),
    )
)
