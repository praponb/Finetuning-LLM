from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 1800) & ~F.col("txn_type").isin(["DEBIT", "FEE", "REFUND"]))
    .select(
        "id",
        "is_flagged",
        F.date_trunc("year", "txn_time").alias("txn_time_period"),
    )
)
