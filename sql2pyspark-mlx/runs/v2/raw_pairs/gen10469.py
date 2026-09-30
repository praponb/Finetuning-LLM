from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "txn_type",
        "merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("merchant", F.col("is_flagged_label").desc())
)
