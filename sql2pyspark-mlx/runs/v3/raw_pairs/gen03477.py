from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("a.opened_date") < "2025-10-01")
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.when(F.col("t.txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
    )
    .orderBy("status")
    .limit(20)
)
