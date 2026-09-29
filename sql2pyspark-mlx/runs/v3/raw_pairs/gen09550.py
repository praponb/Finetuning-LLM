from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
        F.count(F.when(F.col("t.txn_type") == "DEBIT", 1)).alias("debit_count"),
    )
    .filter(F.col("amount_range") > 38)
)
