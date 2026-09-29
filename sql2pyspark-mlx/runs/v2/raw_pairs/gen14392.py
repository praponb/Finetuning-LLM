from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "is_flagged",
        "txn_type",
        "txn_time",
        F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0).alias("is_7_eleven"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
