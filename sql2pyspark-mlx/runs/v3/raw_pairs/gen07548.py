from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("amount").between(2400, 2500) | (F.col("txn_type") == "DEBIT"))
)

transactions_stats = (
    src
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
)

result = (
    transactions_stats
    .select("merchant", "7_eleven_amount")
    .orderBy(F.col("merchant").desc())
    .limit(10)
)
