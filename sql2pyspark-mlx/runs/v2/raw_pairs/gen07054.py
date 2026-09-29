from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount") > 2500)
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("as.txn_type_values"),
    )
    .orderBy("account_type")
)
