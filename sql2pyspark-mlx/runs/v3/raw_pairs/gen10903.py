from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "txn_type",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.month("txn_time").alias("txn_time_month"),
    )
)
