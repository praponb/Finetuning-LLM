from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.total_amount") >= 16000)
    .select(F.col("a.customer_id"), F.col("t.amount_count"), F.col("t.total_amount"))
    .orderBy(F.col("amount_count").desc())
)
