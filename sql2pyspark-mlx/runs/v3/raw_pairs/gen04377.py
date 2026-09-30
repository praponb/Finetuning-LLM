from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.amount_range") >= 39)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("tba.max_amount"),
        F.col("tba.amount_range"),
    )
)
