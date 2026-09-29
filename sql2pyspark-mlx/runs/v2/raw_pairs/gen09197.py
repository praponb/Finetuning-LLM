from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.txn_type") == "REFUND")
    .groupBy(F.col("a.account_type"), F.col("a.status"))
    .agg(
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)
