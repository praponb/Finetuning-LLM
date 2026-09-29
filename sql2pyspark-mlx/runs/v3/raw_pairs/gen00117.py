from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .select(
        "txn_type",
        F.upper("merchant").alias("merchant_upper"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("txn_type", "merchant_upper")
)
