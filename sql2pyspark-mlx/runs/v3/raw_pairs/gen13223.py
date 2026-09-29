from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Shell", "7-Eleven"]) & (F.col("txn_type") == "DEBIT"))
    .select(
        "amount",
        F.concat_ws("/", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)
