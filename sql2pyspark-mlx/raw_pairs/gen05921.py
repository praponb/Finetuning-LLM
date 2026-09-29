from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 3100) & ((F.col("account_id") == 15) | F.col("amount").between(2800, 3300)))
    .select(
        "id",
        "txn_type",
        "merchant",
        "amount",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("is_flagged_label", F.col("id").desc())
)
