from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "REFUND"]))
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.avg_amount") >= 3800)
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("as.txn_type_list"),
        F.col("as.avg_amount"),
    )
)
