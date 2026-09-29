from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "REFUND", "DEBIT"]))
    .groupBy("account_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .filter(F.col("account_totals.total_count") > 5)
    .select(
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
        F.col("account_totals.total_count"),
        F.col("account_totals.min_amount"),
    )
    .orderBy(F.col("balance").desc())
)
