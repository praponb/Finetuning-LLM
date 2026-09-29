from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Amazon"]))
    .select(
        "is_flagged",
        "txn_time",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)
