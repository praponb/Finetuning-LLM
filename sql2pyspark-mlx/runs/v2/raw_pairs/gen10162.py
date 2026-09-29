from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Lazada"]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("as.7_eleven_amount"),
        F.col("as.amount_range"),
    )
    .orderBy("opened_date")
)
