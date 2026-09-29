from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-10-13 16:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.upper("txn_type").alias("txn_type_upper"),
    )
)

result = (
    base
    .groupBy("account_id")
    .pivot("txn_type", ["REFUND", "DEBIT"])
    .agg(F.avg("amount_filled").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
