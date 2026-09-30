from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
        F.date_trunc("week", "txn_time").alias("txn_time_period"),
    )
)
