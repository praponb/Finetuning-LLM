from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "inner")
    .select(
        F.col("accounts.account_type"),
        F.col("transactions_by_account.starbucks_count"),
        F.col("transactions_by_account.unique_accounts"),
    )
    .orderBy(F.col("starbucks_count").desc())
)
