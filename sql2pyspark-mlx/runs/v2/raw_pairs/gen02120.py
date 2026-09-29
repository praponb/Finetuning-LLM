from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .select(
        "id",
        "txn_time",
        "merchant",
        "amount",
        F.concat("txn_type", F.lit(" - "), "merchant").alias("txn_type_merchant"),
    )
    .orderBy("id")
    .limit(10)
)
