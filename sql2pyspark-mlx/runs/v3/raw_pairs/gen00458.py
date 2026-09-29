from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
        F.col("account_stats.stddev_amount"),
        F.col("account_stats.lazada_count"),
    )
)
