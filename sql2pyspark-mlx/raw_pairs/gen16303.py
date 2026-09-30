from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("transactions.merchant") == "Amazon")
    .groupBy(F.col("accounts.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("transactions.txn_type"))).alias("txn_type_list"),
        F.sum(F.when(F.col("transactions.txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
)
