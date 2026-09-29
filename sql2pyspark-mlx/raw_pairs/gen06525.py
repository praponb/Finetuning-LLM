from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.col("t.fee_count"),
        F.col("t.txn_type_values"),
    )
    .orderBy("txn_type_values", "id")
    .limit(5)
)
