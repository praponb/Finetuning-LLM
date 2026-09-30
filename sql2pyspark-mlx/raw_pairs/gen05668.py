from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-05-01")
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("A.amount_range"),
        F.col("A.avg_amount"),
    )
)
