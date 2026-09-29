from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([3, 5, 12, 34]))
    .groupBy("account_id")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("TransactionsByAccount"), F.col("accounts.id") == F.col("TransactionsByAccount.account_id"), "inner")
    .select(F.col("accounts.balance"), F.col("TransactionsByAccount.merchant_list"))
)
