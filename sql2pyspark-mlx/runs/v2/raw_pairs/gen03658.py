from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("as.stddev_amount"),
        F.col("as.txn_type_values"),
    )
    .orderBy("txn_type_values")
)
