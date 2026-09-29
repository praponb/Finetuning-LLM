from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        F.concat_ws(",", "txn_type", "merchant").alias("txn_type_merchant_key"),
        F.lower("merchant").alias("merchant_lower"),
    )
    .orderBy("txn_type_merchant_key")
)
