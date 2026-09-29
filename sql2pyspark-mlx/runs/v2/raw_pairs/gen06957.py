from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") | ~F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "txn_type",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
