from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.unique_merchants") > 9)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("as.unique_merchants"),
    )
)
