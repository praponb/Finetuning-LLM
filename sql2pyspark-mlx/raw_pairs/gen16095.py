from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED"]))
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", F.col("t.amount")).otherwise(0)).alias("credit_amount"),
    )
    .filter(F.col("credit_amount") > 33)
)
