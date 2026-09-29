from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 3000) & (F.col("txn_type") == "REFUND"))
    .select(
        "txn_type",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("txn_type").desc(), "txn_time")
)
