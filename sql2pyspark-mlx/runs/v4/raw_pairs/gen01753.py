from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "is_flagged",
        "merchant",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
    )
    .orderBy("merchant", "txn_time_label")
)
