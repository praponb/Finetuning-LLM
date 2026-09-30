from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2024-06-30")
    .select(
        "id",
        "txn_time",
        "account_id",
        "amount",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
    )
)
