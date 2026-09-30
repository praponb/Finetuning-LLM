from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("merchant") == "7-Eleven") & (F.to_date("txn_time") >= "2023-09-11"))
)

summary = (
    base
    .groupBy("is_flagged")
    .agg(
        F.count("amount").alias("amount_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = summary.select("is_flagged", "amount_count", "min_amount")
