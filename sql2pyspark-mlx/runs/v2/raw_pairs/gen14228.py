from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_range") > 57)
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.stddev_amount"),
        F.col("t.amount_range"),
    )
    .orderBy("balance")
)
