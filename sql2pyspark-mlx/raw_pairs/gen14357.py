from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)
