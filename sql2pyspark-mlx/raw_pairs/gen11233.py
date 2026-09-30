from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("at.amount_range"),
        F.col("at.latest_txn_time"),
    )
    .orderBy(F.col("id").desc(), "latest_txn_time")
    .limit(1)
)
