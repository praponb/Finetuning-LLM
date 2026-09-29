from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.stddev_amount") >= 21)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("t.min_amount"),
        F.col("t.stddev_amount"),
    )
    .orderBy("account_type", F.col("min_amount").desc())
)
