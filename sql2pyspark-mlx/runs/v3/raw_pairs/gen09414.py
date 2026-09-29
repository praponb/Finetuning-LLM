from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "inner")
    .filter(F.col("transactions_by_account.amount_count") > 9)
    .select(
        F.col("accounts.opened_date"),
        F.col("transactions_by_account.amount_count"),
    )
    .orderBy("opened_date")
)
