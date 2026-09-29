from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "left")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("T.unique_merchants"))
)
