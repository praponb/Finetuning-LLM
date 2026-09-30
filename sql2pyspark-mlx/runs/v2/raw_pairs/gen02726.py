from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "left")
    .filter(F.col("transactions_by_account.unique_accounts") > 7)
    .select(
        F.col("accounts.account_type"),
        F.col("transactions_by_account.starbucks_amount"),
        F.col("transactions_by_account.unique_accounts"),
    )
    .orderBy(F.col("account_type").desc())
)
