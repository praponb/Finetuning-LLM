from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "DEBIT") & F.col("is_flagged"))
    .select(
        "is_flagged",
        "amount",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
