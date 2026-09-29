from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.balance"), F.col("tba.fee_amount"))
)
