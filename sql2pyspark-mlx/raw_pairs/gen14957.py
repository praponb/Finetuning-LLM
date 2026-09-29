from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") | (F.col("merchant") != "7-Eleven"))
    .select(
        "id",
        "txn_type",
        "txn_time",
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
    )
)
