from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 6, 14, 18]) & (F.col("account_id") == 5))
    .select(
        "is_flagged",
        "txn_time",
        "amount",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
    )
    .orderBy(F.col("amount").desc(), "txn_time_period")
)
