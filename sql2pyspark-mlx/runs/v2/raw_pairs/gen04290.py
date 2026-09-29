from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id").isin([4, 30]))
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("t.txn_type_list"),
        F.col("t.unique_txn_types"),
    )
    .orderBy(F.col("unique_txn_types").desc())
)
