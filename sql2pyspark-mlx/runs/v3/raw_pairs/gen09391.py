from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.balance") < 33000)
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.when(F.col("t.merchant") == "Lazada", 1)).alias("lazada_count"),
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .orderBy("debit_count", F.col("segment").desc())
)
