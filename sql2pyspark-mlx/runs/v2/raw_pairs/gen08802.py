from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.status") == "FROZEN")
    .groupBy(F.col("accounts.id"), F.col("accounts.status"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("transactions.txn_type")))).alias("txn_type_values"),
    )
    .orderBy(F.col("status").desc())
)
