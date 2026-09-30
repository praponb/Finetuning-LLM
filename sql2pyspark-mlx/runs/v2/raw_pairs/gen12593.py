from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id") == 21)
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.amount_count") >= 7)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("at.amount_count"),
        F.col("at.avg_amount"),
    )
    .orderBy(F.col("amount_count").desc())
)
