from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.unique_txn_types") >= 8)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("as.unique_txn_types"),
        F.col("as.txn_type_list"),
    )
    .orderBy("status", F.col("unique_txn_types").desc())
)
