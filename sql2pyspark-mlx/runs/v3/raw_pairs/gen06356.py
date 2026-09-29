from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.stddev_amount") >= 70)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.amount_count"),
        F.col("t.stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
