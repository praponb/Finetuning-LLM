from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Starbucks", "Lazada"]))
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.balance"), F.col("t.amount_range"), F.col("t.avg_amount"))
    .orderBy(F.col("balance").desc())
)
