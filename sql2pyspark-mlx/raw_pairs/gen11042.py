from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.balance") <= 76000)
    .groupBy(F.col("a.customer_id"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
)
