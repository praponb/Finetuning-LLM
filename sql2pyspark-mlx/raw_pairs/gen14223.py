from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.avg_amount") > 1800)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("at.avg_amount"),
        F.col("at.amount_range"),
    )
    .orderBy("amount_range", "avg_amount", "id")
    .limit(50)
)
