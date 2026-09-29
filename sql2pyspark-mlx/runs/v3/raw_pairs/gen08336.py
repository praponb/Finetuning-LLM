from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("t.amount_count"),
        F.col("t.amount_range"),
    )
)
