from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull() | (F.to_date("txn_time") >= "2024-10-01"))
    .select(
        "txn_type",
        "amount",
        F.concat("txn_type", F.lit(" "), "merchant").alias("txn_type_merchant"),
    )
)
