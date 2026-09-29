from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("TransactionsByAccount"), F.col("accounts.id") == F.col("TransactionsByAccount.account_id"), "left")
    .filter(F.col("TransactionsByAccount.stddev_amount") > 68)
    .select(
        F.col("accounts.account_type"),
        F.col("TransactionsByAccount.stddev_amount"),
        F.col("TransactionsByAccount.merchant_list"),
    )
    .orderBy(F.col("account_type").desc())
)
