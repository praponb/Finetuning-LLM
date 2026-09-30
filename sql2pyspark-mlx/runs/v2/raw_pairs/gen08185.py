from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "amount",
        "txn_type",
        F.to_date("txn_time").alias("txn_time_date"),
        F.month("txn_time").alias("txn_time_month"),
    )
)
