from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("txn_time") >= "2023-05-01")

is_flagged_summary = (
    valid_transactions
    .groupBy("is_flagged")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    is_flagged_summary
    .filter(F.col("amount_count") < 6)
    .orderBy("amount_count")
)
