from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.7_eleven_amount") > 44)
    .select(F.col("a.id"), F.col("a.account_type"), F.col("t.7_eleven_amount"))
    .orderBy("7_eleven_amount")
)
