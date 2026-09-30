from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter((F.col("t.amount") >= 1100) | ~F.col("t.txn_type").isin(["FEE", "REFUND", "DEBIT"]))
    .select(
        F.col("t.is_flagged"),
        F.col("t.merchant"),
        F.col("a.balance"),
        F.col("c.name"),
    )
)
