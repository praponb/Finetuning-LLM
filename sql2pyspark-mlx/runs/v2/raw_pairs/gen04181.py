from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "CREDIT", "REFUND"]))
    .select(
        "txn_type",
        F.date_trunc("year", "txn_time").alias("txn_time_period"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
