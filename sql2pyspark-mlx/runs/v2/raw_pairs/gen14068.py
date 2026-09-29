from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") | (F.col("txn_type") == "DEBIT"))
    .select(
        "id",
        "amount",
        F.lower("txn_type").alias("txn_type_lower"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("txn_type_lower")
)
