from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.avg_amount") > 22)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("at.avg_amount"),
        F.col("at.amount_count"),
    )
    .orderBy(F.col("amount_count").desc(), F.col("id").desc())
)
