from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "id",
        "txn_type",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
    )
)
