from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "txn_time",
        F.concat_ws(",", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
    .orderBy(F.col("txn_time").desc())
)
