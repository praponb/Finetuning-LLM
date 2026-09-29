from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("t.txn_type").isin(["DEBIT", "CREDIT", "FEE"]))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.country"),
        F.col("c.city"),
    )
)
