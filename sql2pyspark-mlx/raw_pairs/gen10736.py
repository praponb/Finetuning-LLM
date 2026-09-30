from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.amount_range") > 57)
    .select(F.col("a.opened_date"), F.col("t.amount_range"), F.col("t.last_txn_time"))
    .orderBy(F.col("last_txn_time").desc())
)
