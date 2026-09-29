from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
