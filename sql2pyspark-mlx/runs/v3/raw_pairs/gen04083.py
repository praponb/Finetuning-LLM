from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 23)
    .select(
        "id",
        "txn_type",
        "merchant",
        "txn_time",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.round((F.col("amount") * 1.15), 2).alias("amount_adjusted"),
    )
)
