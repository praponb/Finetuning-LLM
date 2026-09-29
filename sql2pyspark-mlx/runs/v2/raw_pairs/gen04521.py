from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("t.txn_time") < "2025-10-13")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
)
