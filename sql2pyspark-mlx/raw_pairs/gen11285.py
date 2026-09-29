from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .groupBy(F.col("accounts.account_type"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.count(F.when(F.col("transactions.merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.sum(F.when(F.col("transactions.txn_type") == "CREDIT", F.col("transactions.amount")).otherwise(0)).alias("credit_amount"),
    )
)
