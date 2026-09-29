from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
        F.col("account_totals.amazon_count"),
    )
    .orderBy("customer_id")
)
