from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant") != "Amazon")
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.refund_count") >= 6)
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("t.txn_type_list"),
        F.col("t.refund_count"),
    )
    .orderBy(F.col("refund_count").desc())
)
