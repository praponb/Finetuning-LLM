from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id") == 3)
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("tba.amount_range"),
        F.col("tba.avg_amount"),
    )
    .orderBy(F.col("amount_range").desc())
)
