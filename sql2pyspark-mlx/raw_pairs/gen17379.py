from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-02-27")
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_range") >= 75)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("t.stddev_amount"),
        F.col("t.amount_range"),
    )
    .orderBy("amount_range", "balance")
)
