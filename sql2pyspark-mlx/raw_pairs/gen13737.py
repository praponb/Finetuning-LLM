from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "FEE"]))
    .select(
        "merchant",
        F.to_date("txn_time").alias("txn_time_date"),
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
    .orderBy("txn_time_date", "merchant_txn_type_key")
)
