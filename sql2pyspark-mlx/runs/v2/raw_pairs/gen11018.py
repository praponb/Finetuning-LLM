from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 10))
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .filter(F.col("account_summary.amount_range") >= 80)
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("account_summary.min_amount"),
        F.col("account_summary.amount_range"),
    )
    .orderBy("min_amount")
)
