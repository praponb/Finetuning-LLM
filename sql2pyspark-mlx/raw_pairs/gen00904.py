from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_type",
        "account_id",
        F.trim("merchant").alias("clean_merchant"),
    )
    .orderBy("txn_type")
)
