from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id").isin([1, 20, 33]))
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.avg_amount") >= 64)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("at.avg_amount"),
    )
)
