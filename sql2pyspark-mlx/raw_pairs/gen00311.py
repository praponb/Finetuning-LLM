from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 2600)
    .select(
        "id",
        "txn_type",
        "txn_time",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
    )
)
