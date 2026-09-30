from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("is_flagged") & (F.col("txn_type") == "REFUND")) | (F.col("txn_type") != "FEE"))
    .select(
        "id",
        "amount",
        "txn_time",
        "txn_type",
        F.upper("txn_type").alias("txn_type_upper"),
    )
)
