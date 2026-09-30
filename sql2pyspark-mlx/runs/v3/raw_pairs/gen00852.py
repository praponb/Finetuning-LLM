from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("account_id").isin([8, 24, 38]))

summary = (
    valid_transactions
    .groupBy("merchant", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.max("amount").alias("max_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    summary
    .select("merchant", "txn_month", "max_amount", "amount_range")
    .orderBy(F.col("merchant").desc())
)
