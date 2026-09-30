from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_count") > 5)
    .select(F.col("a.customer_id"), F.col("t.amount_count"), F.col("t.last_txn_time"))
    .orderBy(F.col("amount_count").desc())
)
