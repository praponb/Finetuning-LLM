from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 16) & F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "merchant",
        "amount",
        F.date_trunc("month", "txn_time").alias("txn_time_period"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(10)
)
