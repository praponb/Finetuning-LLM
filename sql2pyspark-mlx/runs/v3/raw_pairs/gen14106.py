from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.unique_merchants") >= 5)
    .select(
        F.col("a.account_type"),
        F.col("t.txn_type_list"),
        F.col("t.unique_merchants"),
    )
    .orderBy("account_type")
)
