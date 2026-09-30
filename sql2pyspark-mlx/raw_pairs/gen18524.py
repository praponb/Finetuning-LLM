from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type").isin(["CHECKING", "SAVINGS"]))
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
)
