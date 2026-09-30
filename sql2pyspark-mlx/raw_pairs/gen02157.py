from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Shell"]))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("tba.merchant_values"),
        F.col("tba.min_txn_time"),
    )
    .orderBy(F.col("min_txn_time").desc())
)
