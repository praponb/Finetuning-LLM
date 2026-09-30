from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type") == "CHECKING")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", F.col("t.amount")).otherwise(0)).alias("debit_amount"),
        F.max(F.col("t.txn_time")).alias("latest_txn_time"),
    )
    .orderBy("debit_amount")
)
