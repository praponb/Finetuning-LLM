from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.when(F.col("t.merchant") == "7-Eleven", F.col("t.amount")).otherwise(0)).alias("7_eleven_amount"),
    )
)
