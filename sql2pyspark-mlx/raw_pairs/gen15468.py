from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.row_count") >= 6)
    .select(F.col("a.balance"), F.col("A.7_eleven_count"), F.col("A.row_count"))
)
