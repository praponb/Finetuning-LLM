from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount").between(3500, 4000))
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.avg_amount") > 3500)
    .select(F.col("a.opened_date"), F.col("as.txn_type_list"), F.col("as.avg_amount"))
    .orderBy("avg_amount")
)
