from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2800) & F.col("merchant").isin(["Lazada", "Amazon"]) & F.col("txn_type").isin(["DEBIT", "REFUND", "FEE"]))
    .select(
        "id",
        "merchant",
        "txn_type",
        "txn_time",
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
    .orderBy(F.col("merchant").desc(), "txn_time", "id")
    .limit(10)
)
