from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("as.merchant_list"),
        F.col("as.min_amount"),
    )
)
