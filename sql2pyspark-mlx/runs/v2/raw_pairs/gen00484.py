from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1900)
    .select(
        "merchant",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.date_trunc("year", "txn_time").alias("txn_time_period"),
    )
)
