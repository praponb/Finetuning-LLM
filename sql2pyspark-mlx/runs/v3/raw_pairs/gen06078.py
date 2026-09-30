from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type") != "FEE")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("as.merchant_values"),
    )
    .orderBy("balance")
)
