from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "txn_type",
        "merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("amount", "id")
    .limit(50)
)
