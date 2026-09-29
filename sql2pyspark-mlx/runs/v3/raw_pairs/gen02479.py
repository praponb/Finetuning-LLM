from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("TransactionsByAccount"), F.col("accounts.id") == F.col("TransactionsByAccount.account_id"), "inner")
    .filter(F.col("TransactionsByAccount.amount_count") > 4)
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("TransactionsByAccount.total_amount"),
        F.col("TransactionsByAccount.amount_count"),
    )
    .orderBy(F.col("balance").desc())
)
