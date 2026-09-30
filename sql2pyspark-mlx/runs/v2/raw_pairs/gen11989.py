from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.account_id") == 5)
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
        F.sum(F.when(F.col("t.merchant") == "Amazon", F.col("t.amount")).otherwise(0)).alias("amazon_amount"),
    )
    .filter(F.col("amazon_amount") >= 76)
    .orderBy("amazon_amount", "id", "account_type")
    .limit(5)
)
