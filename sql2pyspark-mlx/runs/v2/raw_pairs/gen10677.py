from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(F.col("accounts.balance"), F.col("t.max_amount"), F.col("t.amount_count"))
)
