from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "inner")
    .filter(F.col("account_stats.grab_count") >= 5)
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("account_stats.grab_count"),
    )
)
